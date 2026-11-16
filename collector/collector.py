import asyncio
import json
import logging
import os
import uuid

import websockets
from confluent_kafka import Producer

URL = "wss://api.upbit.com/websocket/v1"
CODES = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
TOPIC = "trades.raw"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")

producer = Producer({
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    "enable.idempotence": True,   # 재전송 시 브로커 쪽 중복 방지
    "linger.ms": 50,              # 50ms 모아서 묶음 전송
})


def on_delivery(err, msg):
    if err is not None:
        log.error("전송 실패: %s", err)


async def collect():
    backoff = 1
    count = 0
    while True:
        try:
            async with websockets.connect(URL, ping_interval=30) as ws:
                request = [
                    {"ticket": str(uuid.uuid4())},
                    {"type": "trade", "codes": CODES},
                    {"format": "DEFAULT"},
                ]
                await ws.send(json.dumps(request))
                log.info("연결됨, 구독: %s", CODES)
                backoff = 1

                async for raw in ws:
                    data = json.loads(raw)
                    if data.get("type") != "trade":
                        continue
                    producer.produce(TOPIC, key=data["code"], value=raw, on_delivery=on_delivery)
                    producer.poll(0)
                    count += 1
                    if count % 100 == 0:
                        log.info("누적 %d건 전송", count)
        except Exception as e:
            log.warning("연결 끊김(%s), %d초 후 재연결", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(collect())
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)
        log.info("종료")