import psycopg2
from psycopg2 import sql
from typing import List
from models.sensor import Sensor
from models.position import Position
from models.measurement_type import MeasurementType
from models.measurement import Measurement


class DBConfig:
    SENSOR_TABLE = "sensor"
    SENSOR_MEASUREMENT_TYPE_TABLE = "sensor_measurement_types"
    MEASUREMENT_TABLE = "measurement" 
    
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        
    def to_dict(self):
        return {
            'dbname': self.dbname,
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'port': self.port
            }

class SensorDB:
    def __init__(self, config: DBConfig):
        self.config = config
        self.connection = None

    def connect(self):
        """Establish a connection to the database."""
        if not self.connection:
            try:
                self.connection = psycopg2.connect(**self.config.to_dict())
            except Exception as e:
                print(f"Error connencting to database: {e}")

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def insert_sensor(self, sensor: Sensor) -> Sensor:
        """
        Adds a new sensor to the database.

        :param db_config: Dictionary with database connection information (dbname, user, password, host, port)
        :param sensor_data: Dictionary with sensor data (sensor_id, additional_information, original_id, position, sensor_type, source)
        :return: True if the sensor was successfully added, False in case of errors
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            insert_query = sql.SQL("""
                INSERT INTO {table} (additional_information, original_id, position, sensor_type, source)
                VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s)
                RETURNING sensor_id
            """).format(table=sql.Identifier(DBConfig.SENSOR_TABLE))

            cursor.execute(insert_query, (
                sensor.additional_information,
                sensor.original_id,
                sensor.position.longitude,
                sensor.position.latitude,
                sensor.sensor_type,
                sensor.source
            ))
            id = cursor.fetchone()[0]
            self.connection.commit()
            sensor.set_sensor_id(id)
            print(f"Sensor {id} added successfully.")
            return sensor

        except Exception as e:
            print(f"Error adding sensor: {e}")
            return sensor

        finally:
            if cursor:
                cursor.close()
                self.close
                
    def upsert_sensor(self, sensor: Sensor) -> Sensor:
        """
        Inserts a new sensor or updates an existing sensor based on original_id and source.

        :param sensor: Sensor object
        :return: The inserted or updated Sensor object (with sensor_id set)
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            # Check if sensor exists
            select_query = sql.SQL("""
                SELECT sensor_id FROM {table}
                WHERE original_id = %s AND source = %s
            """).format(table=sql.Identifier(DBConfig.SENSOR_TABLE))

            cursor.execute(select_query, (sensor.original_id, sensor.source))
            result = cursor.fetchone()

            if result:
                # Update existing sensor
                sensor_id = result[0]
                update_query = sql.SQL("""
                    UPDATE {table}
                    SET additional_information = %s,
                        position = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        sensor_type = %s
                    WHERE sensor_id = %s
                """).format(table=sql.Identifier(DBConfig.SENSOR_TABLE))

                cursor.execute(update_query, (
                    sensor.additional_information,
                    sensor.position.longitude,
                    sensor.position.latitude,
                    sensor.sensor_type,
                    sensor_id
                ))
                self.connection.commit()
                sensor.set_sensor_id(sensor_id)
                print(f"Sensor {sensor_id} updated successfully.")
            else:
                # Insert new sensor
                sensor = self.insert_sensor(sensor)
            return sensor

        except Exception as e:
            print(f"Error upserting sensor: {e}")
            return sensor

        finally:
            if cursor:
                cursor.close()
                self.close()
    
    def add_measurment_type_for_sensor(self, sensor: Sensor, measurement_type: MeasurementType) -> bool:
        """
        Links a sensor to a measurement type in the database.

        :param sensor: Sensor object
        :param measurement_type: MeasurementType
        :return: True if the link was successfully added, False otherwise
        """
        
        if not isinstance(measurement_type, MeasurementType):
            if isinstance(measurement_type, int) and MeasurementType.is_valid_type(measurement_type):
                measurement_type = MeasurementType(measurement_type)
            else:
                raise Exception(f"Invalid measurement type: {measurement_type}")
        
        try:
            self.connect()
            cursor = self.connection.cursor()

            insert_query = sql.SQL("""
                INSERT INTO {table} (sensor_id, measurement_type)
                VALUES (%s, %s)
            """).format(table=sql.Identifier(DBConfig.SENSOR_MEASUREMENT_TYPE_TABLE))

            cursor.execute(insert_query, (sensor.sensor_id, measurement_type.value))
            self.connection.commit()
            print(f"Linked sensor {sensor.sensor_id} to measurement type {measurement_type} successfully.")
            return True

        except Exception as e:
            print(f"Error linking sensor to measurement type: {e}")
            return False

        finally:
            if cursor:
                cursor.close()
                self.close()
    
    def get_sensor_by_original_id_and_source(self, original_id: str, source: str) -> Sensor:
        """
        Retrieves a sensor from the database by its original_id and source.

        :param original_id: The original ID of the sensor
        :param source: The source of the sensor
        :return: Sensor object if found, None otherwise
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            select_query = sql.SQL("""
                SELECT sensor_id, additional_information, original_id, 
                       ST_AsText(position) AS position_wkt, 
                       sensor_type, source
                FROM {table}
                WHERE original_id = %s AND source = %s
            """).format(table=sql.Identifier(DBConfig.SENSOR_TABLE))

            cursor.execute(select_query, (original_id, source))
            result = cursor.fetchone()

            if result:
                position_wkt = result[3]
                if not position_wkt.startswith('POINT'):
                    raise Exception(f"Sensor {result[0]} has invalid position: {position_wkt}")
                
                coords = position_wkt[6:-1].split()  # Entfernt 'POINT(' und ')'
                longitude, latitude = map(float, coords)
                    
                sensor = Sensor(
                    sensor_id=result[0],
                    additional_information=result[1],
                    original_id=result[2],
                    position=Position(longitude=longitude, latitude=latitude),
                    sensor_type=result[4],
                    source=result[5]
                )
                print(f"Sensor {sensor.sensor_id} retrieved successfully.")
                return sensor
            else:
                print("No sensor found with the given original_id and source.")
                return None

        except Exception as e:
            print(f"Error retrieving sensor: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
                self.close()
    
    def insert_measurement(self, measurement: Measurement) -> int:
        """
        Adds a new measurement to the database.

        :param measurement: Measurement object
        :return: The measurement_id if the measurement was successfully added, -1 otherwise
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            insert_query = sql.SQL("""
                INSERT INTO {table} (measurement_type, position, timestamp, unit, value, sensor_id)
                VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s)
                RETURNING measurement_id
            """).format(table=sql.Identifier(DBConfig.MEASUREMENT_TABLE))

            cursor.execute(insert_query, (
                measurement.measurement_type,
                measurement.position.longitude,
                measurement.position.latitude,
                measurement.timestamp,
                measurement.unit,
                measurement.value,
                measurement.sensor_id
            ))
            measurement_id = cursor.fetchone()[0]
            self.connection.commit()
            print(f"Measurement {measurement_id} added successfully.")
            return measurement_id

        except Exception as e:
            print(f"Error adding measurement: {e}")
            return -1

        finally:
            if cursor:
                cursor.close()
                self.close()
    
    def insert_batch_measurements(self, measurements: List[Measurement]) -> int:
        """
        Adds a batch of measurements to the database.

        :param measurements: List of Measurement objects
        :return: The number of successfully added measurements
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            insert_query = sql.SQL("""
                INSERT INTO {table} (measurement_type, position, timestamp, unit, value, sensor_id)
                VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s)
            """).format(table=sql.Identifier(DBConfig.MEASUREMENT_TABLE))

            batch_data = [
                (
                    measurement.measurement_type,
                    measurement.position.longitude,
                    measurement.position.latitude,
                    measurement.timestamp,
                    measurement.unit,
                    measurement.value,
                    measurement.sensor_id
                )
                for measurement in measurements
            ]

            cursor.executemany(insert_query, batch_data)
            self.connection.commit()
            print(f"{cursor.rowcount} measurements added successfully.")
            return cursor.rowcount

        except Exception as e:
            print(f"Error adding batch measurements: {e}")
            return 0

        finally:
            if cursor:
                cursor.close()
                self.close()
    
    def clear_measurements_for_sensor(self, sensor_id: int) -> int:
        """
        Deletes all measurements for a given sensor from the database.

        :param sensor_id: The ID of the sensor
        :return: The number of deleted measurements
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            delete_query = sql.SQL("""
                DELETE FROM {table}
                WHERE sensor_id = %s
                RETURNING *
            """).format(table=sql.Identifier(DBConfig.MEASUREMENT_TABLE))

            cursor.execute(delete_query, (sensor_id,))
            deleted_rows = cursor.rowcount
            self.connection.commit()
            print(f"{deleted_rows} measurements for sensor {sensor_id} deleted successfully.")
            return deleted_rows

        except Exception as e:
            print(f"Error deleting measurements for sensor {sensor_id}: {e}")
            return 0

        finally:
            if cursor:
                cursor.close()
                self.close()
                
    
    def get_measurements_for_sensor(self, sensor_id: int, measurement_type: int = None, from_timestamp: str = None, to_timestamp: str = None) -> List[Measurement]:
        """
        Retrieves all measurements for a given sensor, optionally filtered by measurement type and/or from a specific timestamp range.

        :param sensor_id: The ID of the sensor
        :param measurement_type: Optional measurement type to filter by
        :param from_timestamp: Optional timestamp to filter measurements from (inclusive). The timestamp should be in the ISO 8601 format (YYYY-MM-DD HH:MI:SS)
        :param to_timestamp: Optional timestamp to filter measurements up to (inclusive). The timestamp should be in the ISO 8601 format (YYYY-MM-DD HH:MI:SS)
        :return: A list of measurements
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            query = sql.SQL("""
                SELECT measurement_id, measurement_type, ST_AsText(position) AS position_wkt, 
                       timestamp, unit, value, sensor_id
                FROM {table}
                WHERE sensor_id = %s
            """).format(table=sql.Identifier(DBConfig.MEASUREMENT_TABLE))

            params = [sensor_id]

            if measurement_type is not None:
                query += sql.SQL(" AND measurement_type = %s")
                params.append(measurement_type)

            if from_timestamp is not None:
                query += sql.SQL(" AND timestamp >= %s")
                params.append(from_timestamp)

            if to_timestamp is not None:
                query += sql.SQL(" AND timestamp <= %s")
                params.append(to_timestamp)

            cursor.execute(query, tuple(params))
            results = cursor.fetchall()

            measurements = []
            for result in results:
                measurement = {
                    "measurement_id": result[0],
                    "measurement_type": result[1],
                    "position": Position.from_wkt_position(result[2]),
                    "timestamp": result[3],
                    "unit": result[4],
                    "value": result[5],
                    "sensor_id": result[6],
                }
                measurements.append(measurement)

            print(f"Retrieved {len(measurements)} {MeasurementType(measurement_type).name if measurement_type is not None else ''} measurements for sensor {sensor_id}.")
            return measurements

        except Exception as e:
            print(f"Error retrieving measurements for sensor {sensor_id}: {e}")
            return []

        finally:
            if cursor:
                cursor.close()
                self.close()
                
    def get_latest_measurement_timestamp(self, sensor_id: int, measurement_type: int) -> str:
        """
        Retrieves the latest timestamp for a given sensor and measurement type.

        :param sensor_id: The ID of the sensor
        :param measurement_type: The measurement type
        :return: The latest timestamp as a string in ISO 8601 format, or None if no measurement exists
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            query = sql.SQL("""
                SELECT MAX(timestamp)
                FROM {table}
                WHERE sensor_id = %s AND measurement_type = %s
            """).format(table=sql.Identifier(DBConfig.MEASUREMENT_TABLE))

            cursor.execute(query, (sensor_id, measurement_type))
            result = cursor.fetchone()
            latest_timestamp = result[0] if result else None

            return latest_timestamp.isoformat() if latest_timestamp else None

        except Exception as e:
            print(f"Error retrieving latest timestamp for sensor {sensor_id} and measurement type {measurement_type}: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            self.close()
            
    def get_sensors_from_area(self, min_lat, min_lon, max_lat, max_lon): 
        """
        Retrieves all sensors within the specified bounding box (min_lat, min_lon, max_lat, max_lon).

        :param min_lat: Minimum latitude of the bounding box
        :param min_lon: Minimum longitude of the bounding box
        :param max_lat: Maximum latitude of the bounding box
        :param max_lon: Maximum longitude of the bounding box
        :return: List of Sensor objects within the area
        """
        try:
            self.connect()
            cursor = self.connection.cursor()

            
            query = sql.SQL("""
                SELECT sensor_id, additional_information, original_id, 
                       ST_AsText(position) AS position_wkt, 
                       sensor_type, source
                FROM {table}
                WHERE ST_Within(
                    position::geometry,
                    ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                )
            """).format(table=sql.Identifier(DBConfig.SENSOR_TABLE))

            cursor.execute(query, (min_lon, min_lat, max_lon, max_lat))
            results = cursor.fetchall()

            sensors = []
            for result in results:
                position_wkt = result[3]
                coords = position_wkt[6:-1].split()
                longitude, latitude = map(float, coords)
                sensor = Sensor(
                    sensor_id=result[0],
                    additional_information=result[1],
                    original_id=result[2],
                    position=Position(longitude=longitude, latitude=latitude),
                    sensor_type=result[4],
                    source=result[5]
                )
                sensors.append(sensor)

            print(f"Retrieved {len(sensors)} sensors from area.")
            return sensors

        except Exception as e:
            print(f"Error retrieving sensors from area: {e}")
            return []

        finally:
            if cursor:
                cursor.close()
            self.close()