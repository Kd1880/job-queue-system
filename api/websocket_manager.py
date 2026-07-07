import json
from fastapi import WebSocket

class WebSocketManager:

  def __init__(self):
    self.active_connections: dict[str, list[WebSocket]] = {}

  async def connect(self,websocket: WebSocket,user_id:str) -> None:
    await websocket.accept()

    if user_id not in self.active_connections:
      self.active_connections[user_id]=[]

    self.active_connections[user_id].append(websocket)
  
  def disconnect(self,websocket: WebSocket, user_id: str) -> None:
    if user_id in self.active_connections:
      self.active_connections[user_id].remove(websocket)

      if not self.active_connections[user_id]:
        del self.active_connections[user_id]


  async def send_to_user(self,user_id:str,message:dict) ->None:
    if user_id not in self.active_connections:
      return
    
    for websocket in list(self.active_connections[user_id]):
      try:
        await websocket.send_text(json.dumps(message))
      except Exception:
        self.disconnect(websocket,user_id)

  async def subscribe_to_redis(self,redis_client) ->None:
    pubsub = redis_client.pubsub()

    await pubsub.subscribe("jobs:updates")

    async for message in pubsub.listen():
      if message["type"]!="message":
        continue
      update =json.loads(message["data"])

      await self.send_to_user(update["user_id"],update)
# SINGLE shared instance — do register banaye toh messages kho jayenge
manager = WebSocketManager()
