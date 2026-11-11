# Update steps

## A. Upload (GitHub, phone)
Extract this ZIP. In your Rip-call repo: Add file -> Upload files -> select main.dart, server.js,
package.json, pubspec.yaml, patch_android.py -> Commit. (Same names replace the old files.)
This starts an APK build (Actions tab) and a new Render deploy.

## B. Free database (Neon)
1. neon.tech -> sign up -> Create project.
2. Copy the connection string (starts with postgresql://).
3. Render -> your service -> Environment -> Add: DATABASE_URL = that string -> Save.

## C. Firebase push (optional, free)
1. console.firebase.google.com -> Add project.
2. Add an Android app with package name: com.ipcalling.ip_calling
3. Project settings -> General: copy Project ID, Project number (= sender ID),
   and the App ID of the Android app, and the Web API key.
4. In GitHub, edit main.dart: put those 4 values into fbApiKey, fbAppId, fbSenderId, fbProjectId. Commit.
5. Project settings -> Service accounts -> Generate new private key. Open the downloaded JSON,
   copy ALL text. Render -> Environment -> FIREBASE_SERVICE_ACCOUNT = that text.
