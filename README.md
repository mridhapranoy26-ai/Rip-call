# IP Calling

Anonymous Android-to-Android internet voice calls (Flutter + WebRTC + Node.js signaling). No login, no OTP.

## 1. Run the signaling server
```
cd server && npm install && npm start
```
Deploy it somewhere public (Render, Railway, Fly.io, a VPS) behind TLS so you get a `wss://` URL.
For voice to work across mobile networks, also run a TURN server (e.g. coturn) and enter it in Settings.

## 2. Build the APK (GitHub Actions)
Push this folder to the repo. Open the **Actions** tab, run **Build APK**, then download the
`ip-calling-apk` artifact from the finished run. (The workflow generates the Android scaffold itself.)

## 3. Use
Install the APK on two phones, open Settings, set the signaling server URL on both,
then dial the other phone's Calling ID (shown on the Home screen).

## Not implemented yet
- Firebase push (calls only ring while the app is open and connected; hook marked in server.js)
- Full-screen incoming notification, foreground service
- Phone contacts import (contacts are saved Calling IDs), custom ringtone picker
- Not compiled or tested: run `flutter analyze` and test on two devices.
