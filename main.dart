import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

enum CS { idle, calling, incoming, connecting, connected }

String fmt(int s) => '${(s ~/ 60).toString().padLeft(2, '0')}:${(s % 60).toString().padLeft(2, '0')}';

class Svc extends ChangeNotifier {
  late SharedPreferences p;
  String myId = '', url = 'ws://10.0.2.2:8080', peer = '', route = 'earpiece';
  String? notice;
  CS state = CS.idle;
  bool muted = false, wasIncoming = false, answered = false, declined = false, online = false, remoteSet = false;
  DateTime? t0;
  Timer? ticker, ringT;
  WebSocketChannel? ws;
  RTCPeerConnection? pc;
  MediaStream? local;
  final pend = <RTCIceCandidate>[];
  List<Map<String, dynamic>> history = [], contacts = [];

  List<Map<String, dynamic>> _load(String k) =>
      (jsonDecode(p.getString(k) ?? '[]') as List).map((e) => Map<String, dynamic>.from(e as Map)).toList();
  Future<void> _save(String k, List l) => p.setString(k, jsonEncode(l));

  Future<void> init() async {
    p = await SharedPreferences.getInstance();
    myId = p.getString('id') ?? (100000000 + Random.secure().nextInt(900000000)).toString();
    await p.setString('id', myId);
    url = p.getString('url') ?? url;
    history = _load('history');
    contacts = _load('contacts');
    connect();
  }

  Future<void> connect() async {
    ws?.sink.close();
    WebSocketChannel? c;
    try {
      c = WebSocketChannel.connect(Uri.parse(url));
      ws = c;
      await c.ready;
    } catch (_) {
      if (c != null) _lost(c);
      return;
    }
    if (ws != c) return;
    online = true;
    notifyListeners();
    c.sink.add(jsonEncode({'t': 'reg', 'id': myId}));
    c.stream.listen((d) => _on(jsonDecode(d as String) as Map<String, dynamic>),
        onDone: () => _lost(c!), onError: (_) => _lost(c!));
  }

  void _lost(WebSocketChannel c) {
    if (ws != c) return;
    online = false;
    notifyListeners();
    Future.delayed(const Duration(seconds: 3), () { if (ws == c) connect(); });
  }

  void send(Map<String, dynamic> m) => ws?.sink.add(jsonEncode(m));

  String nameFor(String id) {
    for (final c in contacts) { if (c['id'] == id) return c['name'] as String; }
    return '';
  }

  Future<bool> _perm() async {
    final r = await [Permission.microphone, Permission.notification, Permission.bluetoothConnect].request();
    return r[Permission.microphone]?.isGranted ?? false;
  }

  Future<String?> call(String to) async {
    if (to.isEmpty) return 'Enter a calling ID';
    if (to == myId) return 'That is your own ID';
    if (!online) return 'Not connected to server';
    if (state != CS.idle) return 'Already in a call';
    if (!await _perm()) return 'Microphone permission needed';
    peer = to; wasIncoming = false; answered = false; declined = false; notice = null;
    state = CS.calling;
    send({'t': 'invite', 'to': to});
    notifyListeners();
    return null;
  }

  Future<void> accept() async {
    ringT?.cancel();
    if (!await _perm()) { reject(); return; }
    answered = true; state = CS.connecting;
    send({'t': 'accept', 'to': peer});
    notifyListeners();
  }

  void reject() { declined = true; send({'t': 'reject', 'to': peer}); _finish(); }
  void hangup() { send({'t': 'bye', 'to': peer}); _finish(); }

  void _ring() {
    ringT?.cancel();
    ringT = Timer.periodic(const Duration(milliseconds: 1500), (_) {
      HapticFeedback.vibrate();
      SystemSound.play(SystemSoundType.alert);
    });
  }

  Future<void> _on(Map<String, dynamic> m) async {
    final t = m['t'], from = m['from'] as String?;
    switch (t) {
      case 'invite':
        if (state != CS.idle) { send({'t': 'reject', 'to': from}); return; }
        peer = from!; wasIncoming = true; answered = false; declined = false; notice = null;
        state = CS.incoming; _ring(); notifyListeners();
        break;
      case 'accept':
        if (from == peer && state == CS.calling) {
          answered = true; state = CS.connecting; notifyListeners();
          await _pc();
          final o = await pc!.createOffer({'offerToReceiveAudio': 1});
          await pc!.setLocalDescription(o);
          send({'t': 'offer', 'to': peer, 'sdp': o.sdp});
        }
        break;
      case 'offer':
        if (from == peer) {
          await _pc();
          await pc!.setRemoteDescription(RTCSessionDescription(m['sdp'] as String, 'offer'));
          await _flush();
          final a = await pc!.createAnswer();
          await pc!.setLocalDescription(a);
          send({'t': 'answer', 'to': peer, 'sdp': a.sdp});
        }
        break;
      case 'answer':
        if (from == peer && pc != null) {
          await pc!.setRemoteDescription(RTCSessionDescription(m['sdp'] as String, 'answer'));
          await _flush();
        }
        break;
      case 'ice':
        final c = m['c'] as Map<String, dynamic>;
        final cand = RTCIceCandidate(c['candidate'] as String?, c['sdpMid'] as String?, c['sdpMLineIndex'] as int?);
        if (pc != null && remoteSet) { await pc!.addCandidate(cand); } else { pend.add(cand); }
        break;
      case 'reject':
        if (state == CS.calling && from == peer) { notice = 'Call declined'; _finish(); }
        break;
      case 'offline':
        if (state == CS.calling) { notice = '$peer is offline'; _finish(); }
        break;
      case 'bye':
        if (from == peer) _finish();
        break;
    }
  }

  Future<void> _flush() async {
    remoteSet = true;
    for (final c in pend) { await pc!.addCandidate(c); }
    pend.clear();
  }

  Future<void> _pc() async {
    local = await navigator.mediaDevices.getUserMedia({'audio': true, 'video': false});
    final ice = <Map<String, dynamic>>[{'urls': 'stun:stun.l.google.com:19302'}];
    final tu = p.getString('turn') ?? '';
    if (tu.isNotEmpty) ice.add({'urls': tu, 'username': p.getString('tuser') ?? '', 'credential': p.getString('tpass') ?? ''});
    pc = await createPeerConnection({'iceServers': ice, 'sdpSemantics': 'unified-plan'});
    for (final t in local!.getAudioTracks()) { await pc!.addTrack(t, local!); }
    pc!.onIceCandidate = (c) {
      if (c.candidate != null) send({'t': 'ice', 'to': peer, 'c': c.toMap()});
    };
    pc!.onConnectionState = (s) {
      if (s == RTCPeerConnectionState.RTCPeerConnectionStateConnected && state != CS.connected) {
        state = CS.connected; t0 = DateTime.now();
        ticker = Timer.periodic(const Duration(seconds: 1), (_) => notifyListeners());
        setRoute('earpiece');
      } else if (s == RTCPeerConnectionState.RTCPeerConnectionStateFailed) {
        notice = 'Connection lost'; hangup();
      }
    };
  }

  Future<void> setRoute(String r) async {
    route = r;
    try {
      if (r == 'speaker') { await Helper.setSpeakerphoneOn(true); }
      else if (r == 'earpiece') { await Helper.setSpeakerphoneOn(false); }
      else { await Helper.setSpeakerphoneOnButPreferBluetooth(); }
    } catch (_) {}
    notifyListeners();
  }

  void toggleMute() {
    muted = !muted;
    for (final t in local?.getAudioTracks() ?? <MediaStreamTrack>[]) { t.enabled = !muted; }
    notifyListeners();
  }

  void _finish() {
    if (state == CS.idle) return;
    final dur = t0 == null ? 0 : DateTime.now().difference(t0!).inSeconds;
    final dir = wasIncoming ? (answered ? 'in' : (declined ? 'rejected' : 'missed')) : 'out';
    history.insert(0, {'peer': peer, 'dir': dir, 'ts': DateTime.now().toIso8601String(), 'dur': dur});
    if (history.length > 200) history.removeLast();
    _save('history', history);
    ticker?.cancel(); ringT?.cancel(); t0 = null;
    pc?.close(); pc = null;
    for (final t in local?.getTracks() ?? <MediaStreamTrack>[]) { t.stop(); }
    local?.dispose(); local = null;
    pend.clear(); remoteSet = false; muted = false; route = 'earpiece';
    state = CS.idle;
    notifyListeners();
  }

  void clearHistory() { history.clear(); _save('history', history); notifyListeners(); }
  void addContact(String name, String id) {
    contacts.add({'name': name, 'id': id, 'fav': false}); _save('contacts', contacts); notifyListeners();
  }
  void toggleFav(Map<String, dynamic> c) { c['fav'] = !(c['fav'] == true); _save('contacts', contacts); notifyListeners(); }
  void removeContact(Map<String, dynamic> c) { contacts.remove(c); _save('contacts', contacts); notifyListeners(); }

  Future<void> saveSettings(String u, String turn, String tuser, String tpass) async {
    url = u.trim();
    await p.setString('url', url); await p.setString('turn', turn.trim());
    await p.setString('tuser', tuser.trim()); await p.setString('tpass', tpass);
    connect();
  }
}

final svc = Svc();

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const App());
}

class App extends StatelessWidget {
  const App({super.key});
  @override
  Widget build(BuildContext context) => MaterialApp(
        title: 'IP Calling',
        debugShowCheckedModeBanner: false,
        theme: ThemeData(
            useMaterial3: true,
            colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF1565C0)),
            scaffoldBackgroundColor: Colors.white),
        builder: (ctx, child) => ListenableBuilder(
            listenable: svc,
            builder: (_, __) => Stack(children: [
                  child!,
                  if (svc.state == CS.incoming)
                    const Positioned.fill(child: Material(child: IncomingScreen()))
                  else if (svc.state != CS.idle)
                    const Positioned.fill(child: Material(child: CallScreen())),
                ])),
        home: const Splash(),
      );
}

class Splash extends StatefulWidget {
  const Splash({super.key});
  @override
  State<Splash> createState() => _SplashState();
}

class _SplashState extends State<Splash> {
  @override
  void initState() { super.initState(); _go(); }
  Future<void> _go() async {
    await svc.init();
    await Future.delayed(const Duration(milliseconds: 800));
    if (!mounted) return;
    Navigator.of(context).pushReplacement(MaterialPageRoute(builder: (_) => const Home()));
  }
  @override
  Widget build(BuildContext context) => const Scaffold(
      backgroundColor: Color(0xFF1565C0),
      body: Center(
          child: Column(mainAxisSize: MainAxisSize.min, children: [
        Icon(Icons.call, size: 80, color: Colors.white),
        SizedBox(height: 12),
        Text('IP Calling', style: TextStyle(color: Colors.white, fontSize: 28, fontWeight: FontWeight.bold)),
      ])));
}

class Home extends StatelessWidget {
  const Home({super.key});
  Widget tile(BuildContext c, IconData i, String l, Widget page) => Card(
      child: InkWell(
          onTap: () => Navigator.push(c, MaterialPageRoute(builder: (_) => page)),
          child: Column(mainAxisAlignment: MainAxisAlignment.center, children: [
            Icon(i, size: 40, color: Theme.of(c).colorScheme.primary),
            const SizedBox(height: 8),
            Text(l),
          ])));
  @override
  Widget build(BuildContext context) => Scaffold(
      appBar: AppBar(title: const Text('IP Calling')),
      body: ListenableBuilder(
          listenable: svc,
          builder: (c, _) => Padding(
              padding: const EdgeInsets.all(16),
              child: Column(children: [
                Card(
                    color: Theme.of(c).colorScheme.primaryContainer,
                    child: Padding(
                        padding: const EdgeInsets.all(20),
                        child: Column(children: [
                          const Text('My Calling ID'),
                          SelectableText(svc.myId, style: const TextStyle(fontSize: 34, fontWeight: FontWeight.bold)),
                          TextButton.icon(
                              onPressed: () => Clipboard.setData(ClipboardData(text: svc.myId)),
                              icon: const Icon(Icons.copy, size: 16),
                              label: const Text('Copy')),
                          Text(svc.online ? 'Connected to server' : 'Server offline - check Settings'),
                        ]))),
                if (svc.notice != null) Padding(padding: const EdgeInsets.all(8), child: Text(svc.notice!)),
                Expanded(
                    child: GridView.count(crossAxisCount: 2, children: [
                  tile(c, Icons.dialpad, 'Dialer', const Dialer()),
                  tile(c, Icons.contacts, 'Contacts', const Contacts()),
                  tile(c, Icons.history, 'Call History', const History()),
                  tile(c, Icons.settings, 'Settings', const Settings()),
                ])),
              ]))));
}

class Dialer extends StatefulWidget {
  const Dialer({super.key});
  @override
  State<Dialer> createState() => _DialerState();
}

class _DialerState extends State<Dialer> {
  String n = '';
  @override
  Widget build(BuildContext context) => Scaffold(
      appBar: AppBar(title: const Text('Dialer')),
      body: Column(children: [
        Padding(padding: const EdgeInsets.all(20), child: Text(n.isEmpty ? 'Enter calling ID' : n, style: const TextStyle(fontSize: 32))),
        Expanded(
            child: GridView.count(
                crossAxisCount: 3,
                childAspectRatio: 1.5,
                children: ['1', '2', '3', '4', '5', '6', '7', '8', '9', '*', '0', '#']
                    .map((k) => TextButton(onPressed: () => setState(() => n += k), child: Text(k, style: const TextStyle(fontSize: 28))))
                    .toList())),
        Padding(
            padding: const EdgeInsets.only(bottom: 24),
            child: Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
              const SizedBox(width: 56),
              FloatingActionButton.large(
                  backgroundColor: Colors.green,
                  onPressed: () async {
                    final e = await svc.call(n);
                    if (e != null && context.mounted) ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(e)));
                  },
                  child: const Icon(Icons.call, color: Colors.white)),
              GestureDetector(
                  onLongPress: () => setState(() => n = ''),
                  child: IconButton(
                      iconSize: 32,
                      onPressed: () => setState(() { if (n.isNotEmpty) n = n.substring(0, n.length - 1); }),
                      icon: const Icon(Icons.backspace_outlined))),
            ])),
      ]));
}

class Contacts extends StatefulWidget {
  const Contacts({super.key});
  @override
  State<Contacts> createState() => _ContactsState();
}

class _ContactsState extends State<Contacts> {
  String q = '';
  void add() {
    final nc = TextEditingController(), ic = TextEditingController();
    showDialog(
        context: context,
        builder: (_) => AlertDialog(
              title: const Text('Add contact'),
              content: Column(mainAxisSize: MainAxisSize.min, children: [
                TextField(controller: nc, decoration: const InputDecoration(labelText: 'Name')),
                TextField(controller: ic, keyboardType: TextInputType.number, decoration: const InputDecoration(labelText: 'Calling ID')),
              ]),
              actions: [
                TextButton(onPressed: () => Navigator.pop(context), child: const Text('Cancel')),
                TextButton(
                    onPressed: () {
                      if (ic.text.trim().isNotEmpty) svc.addContact(nc.text.trim().isEmpty ? ic.text.trim() : nc.text.trim(), ic.text.trim());
                      Navigator.pop(context);
                    },
                    child: const Text('Save')),
              ],
            ));
  }

  @override
  Widget build(BuildContext context) => Scaffold(
      appBar: AppBar(title: const Text('Contacts')),
      floatingActionButton: FloatingActionButton(onPressed: add, child: const Icon(Icons.person_add)),
      body: ListenableBuilder(
          listenable: svc,
          builder: (c, _) {
            final l = svc.contacts.where((e) => '${e['name']}${e['id']}'.toLowerCase().contains(q.toLowerCase())).toList()
              ..sort((a, b) => (b['fav'] == true ? 1 : 0) - (a['fav'] == true ? 1 : 0));
            return Column(children: [
              Padding(
                  padding: const EdgeInsets.all(12),
                  child: TextField(onChanged: (v) => setState(() => q = v), decoration: const InputDecoration(prefixIcon: Icon(Icons.search), hintText: 'Search', border: OutlineInputBorder()))),
              Expanded(
                  child: ListView(
                      children: l
                          .map((e) => ListTile(
                                leading: IconButton(icon: Icon(e['fav'] == true ? Icons.star : Icons.star_border, color: Colors.amber), onPressed: () => svc.toggleFav(e)),
                                title: Text(e['name'] as String),
                                subtitle: Text(e['id'] as String),
                                onLongPress: () => svc.removeContact(e),
                                trailing: IconButton(
                                    icon: const Icon(Icons.call, color: Colors.green),
                                    onPressed: () async {
                                      final err = await svc.call(e['id'] as String);
                                      if (err != null && c.mounted) ScaffoldMessenger.of(c).showSnackBar(SnackBar(content: Text(err)));
                                    }),
                              ))
                          .toList())),
            ]);
          }));
}

class History extends StatelessWidget {
  const History({super.key});
  @override
  Widget build(BuildContext context) => Scaffold(
      appBar: AppBar(title: const Text('Call History'), actions: [IconButton(onPressed: svc.clearHistory, icon: const Icon(Icons.delete_outline))]),
      body: ListenableBuilder(
          listenable: svc,
          builder: (c, _) => ListView(
              children: svc.history.map((h) {
            final d = h['dir'] as String;
            final ts = DateTime.parse(h['ts'] as String).toLocal();
            final n = svc.nameFor(h['peer'] as String);
            return ListTile(
              leading: Icon(d == 'out' ? Icons.call_made : d == 'in' ? Icons.call_received : Icons.call_missed,
                  color: d == 'missed' || d == 'rejected' ? Colors.red : Colors.green),
              title: Text(n.isEmpty ? h['peer'] as String : '$n (${h['peer']})'),
              subtitle: Text('$d - ${ts.toString().substring(0, 16)} - ${fmt(h['dur'] as int)}'),
              onTap: () => svc.call(h['peer'] as String),
            );
          }).toList())));
}

class Settings extends StatefulWidget {
  const Settings({super.key});
  @override
  State<Settings> createState() => _SettingsState();
}

class _SettingsState extends State<Settings> {
  late final u = TextEditingController(text: svc.url);
  late final t = TextEditingController(text: svc.p.getString('turn') ?? '');
  late final tu = TextEditingController(text: svc.p.getString('tuser') ?? '');
  late final tp = TextEditingController(text: svc.p.getString('tpass') ?? '');
  Widget f(TextEditingController c, String l, {bool obscure = false}) => Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: TextField(controller: c, obscureText: obscure, decoration: InputDecoration(labelText: l, border: const OutlineInputBorder())));
  @override
  Widget build(BuildContext context) => Scaffold(
      appBar: AppBar(title: const Text('Settings')),
      body: ListView(padding: const EdgeInsets.all(16), children: [
        Text('Your Calling ID: ${svc.myId}'),
        const SizedBox(height: 16),
        f(u, 'Signaling server (wss://... or ws://...)'),
        f(t, 'TURN URL (optional, turn:host:3478)'),
        f(tu, 'TURN username'),
        f(tp, 'TURN password', obscure: true),
        FilledButton(
            onPressed: () {
              svc.saveSettings(u.text, t.text, tu.text, tp.text);
              ScaffoldMessenger.of(context).showSnackBar(const SnackBar(content: Text('Saved')));
            },
            child: const Text('Save and reconnect')),
        const SizedBox(height: 24),
        const Text('Privacy: no account, no phone number, no recording. Calls are encrypted (DTLS-SRTP). The server only relays signaling for your anonymous ID.'),
        const SizedBox(height: 12),
        const Text('Version 1.0.0'),
      ]));
}

class IncomingScreen extends StatelessWidget {
  const IncomingScreen({super.key});
  @override
  Widget build(BuildContext context) {
    final n = svc.nameFor(svc.peer);
    return Scaffold(
        backgroundColor: const Color(0xFF1565C0),
        body: SafeArea(
            child: Column(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
          const Text('Incoming call', style: TextStyle(color: Colors.white70, fontSize: 20)),
          Column(children: [
            const Icon(Icons.account_circle, size: 110, color: Colors.white),
            Text(n.isEmpty ? svc.peer : n, style: const TextStyle(color: Colors.white, fontSize: 32, fontWeight: FontWeight.bold)),
            if (n.isNotEmpty) Text(svc.peer, style: const TextStyle(color: Colors.white70)),
          ]),
          Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
            FloatingActionButton.large(heroTag: 'r', backgroundColor: Colors.red, onPressed: svc.reject, child: const Icon(Icons.call_end, color: Colors.white)),
            FloatingActionButton.large(heroTag: 'a', backgroundColor: Colors.green, onPressed: svc.accept, child: const Icon(Icons.call, color: Colors.white)),
          ]),
        ])));
  }
}

class CallScreen extends StatelessWidget {
  const CallScreen({super.key});
  Widget r(String k, IconData i, String l) {
    final sel = svc.route == k;
    return Column(children: [
      sel ? IconButton.filled(onPressed: () => svc.setRoute(k), icon: Icon(i)) : IconButton.outlined(onPressed: () => svc.setRoute(k), icon: Icon(i)),
      Text(l),
    ]);
  }

  @override
  Widget build(BuildContext context) {
    final s = svc;
    final status = s.state == CS.calling
        ? 'Calling...'
        : s.state == CS.connecting
            ? 'Connecting...'
            : fmt(DateTime.now().difference(s.t0 ?? DateTime.now()).inSeconds);
    final n = s.nameFor(s.peer);
    return Scaffold(
        body: SafeArea(
            child: Column(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
      Column(children: [
        const Icon(Icons.account_circle, size: 100),
        Text(n.isEmpty ? s.peer : n, style: const TextStyle(fontSize: 30, fontWeight: FontWeight.bold)),
        Text(status, style: const TextStyle(fontSize: 20)),
      ]),
      Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
        r('speaker', Icons.volume_up, 'Speaker'),
        r('earpiece', Icons.phone_in_talk, 'Earpiece'),
        r('bluetooth', Icons.bluetooth_audio, 'Bluetooth'),
      ]),
      Column(children: [
        s.muted ? IconButton.filled(onPressed: s.toggleMute, icon: const Icon(Icons.mic_off)) : IconButton.outlined(onPressed: s.toggleMute, icon: const Icon(Icons.mic)),
        Text(s.muted ? 'Unmute' : 'Mute'),
      ]),
      FloatingActionButton.large(backgroundColor: Colors.red, onPressed: s.hangup, child: const Icon(Icons.call_end, color: Colors.white)),
    ])));
  }
}
