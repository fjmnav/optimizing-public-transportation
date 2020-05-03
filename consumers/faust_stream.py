"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str



app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.table.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)
table = app.Table(
   "transformed-stations",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def process(stream):
    async for event in stream:
        # "line" is the color of the station.
        line = None
        if event.red:
            line = 'red'
        elif event.blue:
            line = 'blue'
        elif event.green:
            line = 'green'
        else:
            logger.warning(f"Line Color Not Specified in {event}")

        # Serialization: Station records -> TransformedStation
        out = TransformedStation(
            station_id=event.station_id,
            station_name=event.station_name,
            order=event.order,
            line=line,
        )

        # add to Table
        table[event.station_id] = out


if __name__ == "__main__":
    app.main()
