import asyncio
import logging
import os
import pandas as pd
from tb_gateway_mqtt import TBGatewayMqttClient
import random

logging.basicConfig(level=logging.DEBUG)

async def send_telemetries(gateway, device_names):
    while True:
        for device_name in device_names:
            telemetry = {"online": 1}  # Randomly choose 0 or 1
            gateway.gw_send_telemetry(device_name, telemetry)
            print(f"Telemetry successfully sent to {device_name}: {telemetry}")
            await asyncio.sleep(1)

async def main():
    gateway = TBGatewayMqttClient("iot.tocloud.kz", 1883, "51kqhLR8qCnma0tvK6rr")
    # gateway = TBGatewayMqttClient("127.0.0.1", 8080, "ktsxox339y11n9anpanb")
    gateway.connect()

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        excel_file_path = os.path.join(script_dir, "devices2.xlsx")  # Change filename accordingly
        device_names_df = pd.read_excel(excel_file_path)
        device_names = device_names_df["deviceName"].tolist()  # Adjust column name here

        await asyncio.gather(
            send_telemetries(gateway, device_names)
        )
    finally:
        gateway.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
