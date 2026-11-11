import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:firebase_messaging/firebase_messaging.dart';
import 'package:cryptography/cryptography.dart';
import 'package:flutter_webrtc/flutter_webrtc.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

enum CS { idle, calling, incoming, connecting, connected }

// ---- Firebase settings: fill these 4 values (see SETUP.md). Leave empty to disable push. ----
const fbApiKey = '';
const fbAppId = '';
const fbSenderId = '';
const fbProjectId = '';

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
  final lr = RTCVideoRenderer(), rr = RTCVideoRenderer();
  bool video = false, camOn = true;
  Timer? noAns;
  String? fcmToken, callCode;
  SimpleKeyPair? kp;
  String myPk = '';
  final keyWaiters = <String, Completer<String?>>{};
  List<Map<String, dynamic>> chats = [];
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
    chats = _load('chatidx');
    await lr.initialize();
    await rr.initialize();
    await _initKeys();
    connect();
    _initFirebase();
  }

  Future<void> connect() async {
    ws?.sink.close();
    if (!url.startsWith('wss://')) {
      online = false;
      notice = 'Server address must start with wss:// (encrypted)';
      notifyListeners();
      return;
    }
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
    c.sink.add(jsonEncode({'t': 'reg', 'id': myId, 'pk': myPk}));
    _regTok();
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

  Future<void> _initFirebase() async {
    if (fbApiKey.isEmpty) return;
    try {
      await Firebase.initializeApp(
          options: const FirebaseOptions(apiKey: fbApiKey, appId: fbAppId, messagingSenderId: fbSenderId, projectId: fbProjectId));
      final m = FirebaseMessaging.instance;
      await m.requestPermission();
      fcmToken = await m.getToken();
      m.onTokenRefresh.listen((t) { fcmToken = t; _regTok(); });
      _regTok();
    } catch (_) {}
  }

  void _regTok() { if (fcmToken != null) send({'t': 'tok', 'token': fcmToken}); }

  List<Map<String, dynamic>> thread(String peer) =>
      (jsonDecode(p.getString('chat_$peer') ?? '[]') as List).map((e) => Map<String, dynamic>.from(e as Map)).toList();

  void _store(String peer, bool me, String text) {
    final l = thread(peer)..add({'me': me, 'text': text, 'ts': DateTime.now().toIso8601String()});
    if (l.length > 500) l.removeRange(0, l.length - 500);
    p.setString('chat_$peer', jsonEncode(l));
    chats.removeWhere((e) => e['peer'] == peer);
    chats.insert(0, {'peer': peer, 'last': text, 'ts': DateTime.now().toIso8601String()});
    _save('chatidx', chats);
    notifyListeners();
  }

  Future<void> _initKeys() async {
    var seed = p.getString('sk');
    if (seed == null) {
      final r = Random.secure();
      seed = base64Encode(List<int>.generate(32, (_) => r.nextInt(256)));
      await p.setString('sk', seed);
    }
    kp = await X25519().newKeyPairFromSeed(base64Decode(seed));
    myPk = base64Encode((await kp!.extractPublicKey()).bytes);
  }

  Future<String?> _peerKey(String peer) async {
    final pinned = p.getString('pk_$peer');
    if (pinned != null) return pinned;
    final w = keyWaiters.putIfAbsent(peer, () => Completer<String?>());
    send({'t': 'getkey', 'to': peer});
    final pk = await w.future.timeout(const Duration(seconds: 8), onTimeout: () => null);
    keyWaiters.remove(peer);
    if (pk == null) return null;
    await p.setString('pk_$peer', pk);
    return pk;
  }

  Future<SecretKey> _shared(String peerPk) async {
    final sec = await X25519().sharedSecretKey(
        keyPair: kp!, remotePublicKey: SimplePublicKey(base64Decode(peerPk), type: KeyPairType.x25519));
    return Hkdf(hmac: Hmac.sha256(), outputLength: 32).deriveKey(secretKey: sec, nonce: utf8.encode('ipcalling-v1'));
  }

  Future<String?> sendMsg(String peer, String text) async {
    if (!online) return 'Not connected to server';
    try {
      final pk = await _peerKey(peer);
      if (pk == null) return 'Security key unavailable for $peer (unknown ID?)';
      final box = await AesGcm.with256bits().encrypt(utf8.encode(text),
          secretKey: await _shared(pk), aad: utf8.encode('$myId>$peer'));
      send({'t': 'msg', 'to': peer, 'c': base64Encode(box.concatenation())});
      _store(peer, true, text);
      return null;
    } catch (_) {
      return 'Encryption failed';
    }
  }

  Future<void> _recv(String from, String c) async {
    var text = '[unreadable message]';
    try {
      final pk = await _peerKey(from);
      if (pk != null) {
        final box = SecretBox.fromConcatenation(base64Decode(c), nonceLength: 12, macLength: 16);
        text = utf8.decode(await AesGcm.with256bits().decrypt(box, secretKey: await _shared(pk), aad: utf8.encode('$from>$myId')));
      }
    } catch (_) {}
    _store(from, false, text);
  }

  Future<String> safety(String a, String b) async {
    final l = [a, b]..sort();
    final h = (await Sha256().hash(utf8.encode(l.join('|')))).bytes;
    return List.generate(6, (i) => (((h[2 * i] << 8) | h[2 * i + 1]) % 10000).toString().padLeft(4, '0')).join(' ');
  }

  Future<void> _calcCode() async {
    try {
      final re = RegExp(r'a=fingerprint:\S+ ([0-9A-Fa-f:]+)');
      final l = (await pc?.getLocalDescription())?.sdp, r = (await pc?.getRemoteDescription())?.sdp;
      final fa = re.firstMatch(l ?? '')?.group(1), fb = re.firstMatch(r ?? '')?.group(1);
      if (fa != null && fb != null) { callCode = await safety(fa, fb); notifyListeners(); }
    } catch (_) {}
  }

  String nameFor(String id) {
    for (final c in contacts) { if (c['id'] == id) return c['name'] as String; }
    return '';
  }

  Future<bool> _perm({bool v = false}) async {
    final r = await [Permission.microphone, Permission.notification, Permission.bluetoothConnect, if (v) Permission.camera].request();
    return (r[Permission.microphone]?.isGranted ?? false) && (!v || (r[Permission.camera]?.isGranted ?? false));
  }

  Future<String?> call(String to, {bool video = false}) async {
    if (to.isEmpty) return 'Enter a calling ID';
    if (to == myId) return 'That is your own ID';
    if (!online) return 'Not connected to server';
    if (state != CS.idle) return 'Already in a call';
    if (!await _perm(v: video)) return 'Microphone/camera permission needed';
    this.video = video;
    peer = to; wasIncoming = false; answered = false; declined = false; notice = null;
    state = CS.calling;
    send({'t': 'invite', 'to': to, 'v': video});
    noAns?.cancel();
    noAns = Timer(const Duration(seconds: 45), () { if (state == CS.calling) { notice = 'No answer'; hangup(); } });
    notifyListeners();
    return null;
  }

  Future<void> accept() async {
    ringT?.cancel();
    if (!await _perm(v: video)) { reject(); return; }
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
        peer = from!; wasIncoming = true; answered = false; declined = false; notice = null; video = m['v'] == true;
        state = CS.incoming; _ring(); notifyListeners();
        break;
      case 'accept':
        if (from == peer && state == CS.calling) {
          answered = true; state = CS.connecting; notifyListeners();
          await _pc();
          final o = await pc!.createOffer({'offerToReceiveAudio': 1, 'offerToReceiveVideo': video ? 1 : 0});
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
      case 'key':
        final w = keyWaiters[from ?? ''];
        if (w != null && !w.isCompleted) w.complete(m['pk'] as String?);
        break;
      case 'err':
        notice = m['msg'] as String?;
        notifyListeners();
        break;
      case 'msg':
        if (from != null && m['c'] is String) _recv(from, m['c'] as String);
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
    local = await navigator.mediaDevices.getUserMedia({'audio': true, 'video': video ? {'facingMode': 'user'} : false});
    lr.srcObject = local;
    final ice = <Map<String, dynamic>>[{'urls': 'stun:stun.l.google.com:19302'}];
    final tu = p.getString('turn') ?? '';
    if (tu.isNotEmpty) ice.add({'urls': tu, 'username': p.getString('tuser') ?? '', 'credential': p.getString('tpass') ?? ''});
    pc = await createPeerConnection({'iceServers': ice, 'sdpSemantics': 'unified-plan'});
    for (final t in local!.getTracks()) { await pc!.addTrack(t, local!); }
    pc!.onTrack = (e) { if (e.streams.isNotEmpty) { rr.srcObject = e.streams[0]; notifyListeners(); } };
    pc!.onIceCandidate = (c) {
      if (c.candidate != null) send({'t': 'ice', 'to': peer, 'c': c.toMap()});
    };
    pc!.onConnectionState = (s) {
      if (s == RTCPeerConnectionState.RTCPeerConnectionStateConnected && state != CS.connected) {
        state = CS.connected; t0 = DateTime.now();
        ticker = Timer.periodic(const Duration(seconds: 1), (_) => notifyListeners());
        setRoute(video ? 'speaker' : 'earpiece'); noAns?.cancel(); _calcCode();
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

  void toggleCam() {
    camOn = !camOn;
    for (final t in local?.getVideoTracks() ?? <MediaStreamTrack>[]) { t.enabled = camOn; }
    notifyListeners();
  }

  void switchCam() {
    final t = local?.getVideoTracks() ?? <MediaStreamTrack>[];
    if (t.isNotEmpty) Helper.switchCamera(t.first);
  }

  void _finish() {
    if (state == CS.idle) return;
    final dur = t0 == null ? 0 : DateTime.now().difference(t0!).inSeconds;
    final dir = wasIncoming ? (answered ? 'in' : (declined ? 'rejected' : 'missed')) : 'out';
    history.insert(0, {'peer': peer, 'dir': dir, 'ts': DateTime.now().toIso8601String(), 'dur': dur});
    if (history.length > 200) history.removeLast();
    _save('history', history);
    ticker?.cancel(); ringT?.cancel(); noAns?.cancel(); t0 = null;
    rr.srcObject = null; lr.srcObject = null; video = false; camOn = true; callCode = null;
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
                  tile(c, Icons.chat, 'Chats', const Chats()),
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
              IconButton(
                  iconSize: 36,
                  onPressed: () async {
                    final e = await svc.call(n, video: true);
                    if (e != null && context.mounted) ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(e)));
                  },
                  icon: const Icon(Icons.videocam, color: Colors.blue)),
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
                                onTap: () => Navigator.push(c, MaterialPageRoute(builder: (_) => ChatPage(e['id'] as String))),
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
          Text(svc.video ? 'Incoming video call' : 'Incoming call', style: const TextStyle(color: Colors.white70, fontSize: 20)),
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
  Widget _videoUi(Svc s) {
    final status = s.state == CS.connected
        ? fmt(DateTime.now().difference(s.t0 ?? DateTime.now()).inSeconds)
        : (s.state == CS.calling ? 'Calling...' : 'Connecting...');
    return Scaffold(
        backgroundColor: Colors.black,
        body: Stack(children: [
          Positioned.fill(child: RTCVideoView(s.rr, objectFit: RTCVideoViewObjectFit.RTCVideoViewObjectFitCover)),
          Positioned(
              right: 16, top: 48, width: 110, height: 150,
              child: ClipRRect(
                  borderRadius: BorderRadius.circular(12),
                  child: RTCVideoView(s.lr, mirror: true, objectFit: RTCVideoViewObjectFit.RTCVideoViewObjectFitCover))),
          Positioned(
              left: 16, top: 48,
              child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
                Text(s.peer, style: const TextStyle(color: Colors.white, fontSize: 22, fontWeight: FontWeight.bold)),
                Text(status, style: const TextStyle(color: Colors.white70)),
                if (s.callCode != null) Text('Security code: ${s.callCode}', style: const TextStyle(color: Colors.white54, fontSize: 12)),
              ])),
          Positioned(
              left: 0, right: 0, bottom: 40,
              child: Row(mainAxisAlignment: MainAxisAlignment.spaceEvenly, children: [
                IconButton.filledTonal(onPressed: s.toggleMute, icon: Icon(s.muted ? Icons.mic_off : Icons.mic)),
                IconButton.filledTonal(onPressed: s.toggleCam, icon: Icon(s.camOn ? Icons.videocam : Icons.videocam_off)),
                IconButton.filledTonal(onPressed: s.switchCam, icon: const Icon(Icons.cameraswitch)),
                IconButton.filledTonal(
                    onPressed: () => s.setRoute(s.route == 'speaker' ? 'earpiece' : 'speaker'),
                    icon: Icon(s.route == 'speaker' ? Icons.volume_up : Icons.phone_in_talk)),
                FloatingActionButton(backgroundColor: Colors.red, onPressed: s.hangup, child: const Icon(Icons.call_end, color: Colors.white)),
              ])),
        ]));
  }

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
    if (s.video) return _videoUi(s);
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
        if (s.callCode != null) Text('Security code: ${s.callCode}', style: const TextStyle(fontSize: 13, color: Colors.black54)),
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

class Chats extends StatelessWidget {
  const Chats({super.key});
  void _new(BuildContext c) {
    final ic = TextEditingController();
    showDialog(
        context: c,
        builder: (dc) => AlertDialog(
              title: const Text('New chat'),
              content: TextField(controller: ic, keyboardType: TextInputType.number, decoration: const InputDecoration(labelText: 'Calling ID')),
              actions: [
                TextButton(
                    onPressed: () {
                      final id = ic.text.trim();
                      Navigator.pop(dc);
                      if (id.isNotEmpty) Navigator.push(c, MaterialPageRoute(builder: (_) => ChatPage(id)));
                    },
                    child: const Text('Open')),
              ],
            ));
  }

  @override
  Widget build(BuildContext context) => Scaffold(
      appBar: AppBar(title: const Text('Chats')),
      floatingActionButton: FloatingActionButton(onPressed: () => _new(context), child: const Icon(Icons.edit)),
      body: ListenableBuilder(
          listenable: svc,
          builder: (c, _) => ListView(
              children: svc.chats.map((e) {
            final id = e['peer'] as String;
            final n = svc.nameFor(id);
            return ListTile(
              leading: const Icon(Icons.account_circle, size: 40),
              title: Text(n.isEmpty ? id : n),
              subtitle: Text(e['last'] as String, maxLines: 1, overflow: TextOverflow.ellipsis),
              onTap: () => Navigator.push(c, MaterialPageRoute(builder: (_) => ChatPage(id))),
            );
          }).toList())));
}

class ChatPage extends StatefulWidget {
  final String peer;
  const ChatPage(this.peer, {super.key});
  @override
  State<ChatPage> createState() => _ChatPageState();
}

class _ChatPageState extends State<ChatPage> {
  final tc = TextEditingController();
  void _snack(String t) => ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(t)));
  Future<void> _call(bool v) async {
    final e = await svc.call(widget.peer, video: v);
    if (e != null && mounted) _snack(e);
  }

  Future<void> _send() async {
    final t = tc.text.trim();
    if (t.isEmpty) return;
    tc.clear();
    final e = await svc.sendMsg(widget.peer, t);
    if (e != null && mounted) { tc.text = t; _snack(e); }
  }

  Future<void> _showCode() async {
    final pk = await svc._peerKey(widget.peer);
    final code = pk == null ? null : await svc.safety(svc.myPk, pk);
    if (!mounted) return;
    showDialog(
        context: context,
        builder: (_) => AlertDialog(
              title: const Text('Security code'),
              content: Text(code == null
                  ? 'Key not available yet'
                  : '$code\n\nYour contact sees the same code on their screen. If the codes match, nobody is intercepting this chat.'),
              actions: [TextButton(onPressed: () => Navigator.pop(context), child: const Text('OK'))],
            ));
  }

  @override
  Widget build(BuildContext context) {
    final n = svc.nameFor(widget.peer);
    return Scaffold(
        appBar: AppBar(title: Text(n.isEmpty ? widget.peer : n), actions: [
          IconButton(icon: const Icon(Icons.lock_outline), onPressed: _showCode),
          IconButton(icon: const Icon(Icons.call), onPressed: () => _call(false)),
          IconButton(icon: const Icon(Icons.videocam), onPressed: () => _call(true)),
        ]),
        body: ListenableBuilder(
            listenable: svc,
            builder: (c, _) {
              final l = svc.thread(widget.peer).reversed.toList();
              return Column(children: [
                Expanded(
                    child: ListView(
                        reverse: true,
                        padding: const EdgeInsets.all(12),
                        children: l
                            .map((m) => Align(
                                alignment: m['me'] == true ? Alignment.centerRight : Alignment.centerLeft,
                                child: Container(
                                    margin: const EdgeInsets.symmetric(vertical: 3),
                                    padding: const EdgeInsets.all(10),
                                    decoration: BoxDecoration(
                                        color: m['me'] == true ? Theme.of(c).colorScheme.primaryContainer : Colors.grey.shade200,
                                        borderRadius: BorderRadius.circular(12)),
                                    child: Text(m['text'] as String))))
                            .toList())),
                SafeArea(
                    child: Padding(
                        padding: const EdgeInsets.all(8),
                        child: Row(children: [
                          Expanded(child: TextField(controller: tc, decoration: const InputDecoration(hintText: 'Message', border: OutlineInputBorder()))),
                          IconButton(icon: const Icon(Icons.send), onPressed: _send),
                        ]))),
              ]);
            }));
  }
}
