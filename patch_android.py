p = 'android/app/src/main/AndroidManifest.xml'
s = open(p).read()
perms = ''.join('<uses-permission android:name="android.permission.%s"/>\n' % x for x in
  ['INTERNET','RECORD_AUDIO','MODIFY_AUDIO_SETTINGS','BLUETOOTH_CONNECT','POST_NOTIFICATIONS','ACCESS_NETWORK_STATE','VIBRATE','WAKE_LOCK'])
s = s.replace('<application', perms + '<application android:usesCleartextTraffic="true"', 1)
s = s.replace('android:label="ip_calling"', 'android:label="IP Calling"')
open(p, 'w').write(s)
