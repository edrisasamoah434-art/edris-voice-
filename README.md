# EDRIS voice relay

A tiny always-on WebSocket server for Twilio ConversationRelay. Twilio does the listening
(Deepgram) and speaking (streaming TTS) on EDRIS's phone calls; for each finished sentence,
this relay asks edrisai.com for EDRIS's reply and streams her words back as they arrive.

It holds no API keys. Environment variables (set on Render):
- `RELAY_SECRET`: shared with edrisai.com
- `EDRIS_URL`: https://edrisai.com

Runs on Render: build `npm install`, start `node server.js`. Health check: `/health`.
