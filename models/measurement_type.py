from enum import Enum, verify, UNIQUE, CONTINUOUS

@verify(UNIQUE, CONTINUOUS)
class MeasurementType(Enum):
    UNKNOWN = -1
    PRESSURE = 0
    TEMPERATURE = 1
    HUMIDITY = 2
    WIND_STRENGTH = 3
    WIND_ANGLE = 4
    GUST_STRENGTH = 5
    GUST_ANGLE = 6
    RAIN_60MIN = 7
    RAIN_24H = 8
    RAIN_LIVE = 9
    PRESSURE_24H = 10
    TEMPERATURE_24H = 11
    HUMIDITY_24H = 12
    WIND_STRENGTH_24H = 13
    GUST_STRENGTH_24H_MAX = 14
    CLOUD_COVERAGE_24H = 15
    PRECIPITATION_TYPE = 16
    SUN_24H = 17
    SNOW_24H = 18
    TEMPERATURE_24H_MIN = 19
    TEMPERATURE_24H_MAX = 20
    HUMIDITY_24H_MIN = 21
    HUMIDITY_24H_MAX = 22
    PRESSURE_24H_MIN = 23
    PRESSURE_24H_MAX = 24
    WIND_ANGLE_24H = 25
    GUST_STRENGTH_24H =26
    GUST_ANGLE_24H = 27

    @staticmethod
    def get_unit_for_type(measurement_type) -> str:
        units = {
            MeasurementType.PRESSURE: "mBar",
            MeasurementType.PRESSURE_24H: "mBar",
            MeasurementType.PRESSURE_24H_MIN: "mBar",
            MeasurementType.PRESSURE_24H_MAX: "mBar",
            MeasurementType.TEMPERATURE: "Celsius",
            MeasurementType.TEMPERATURE_24H: "Celsius",
            MeasurementType.TEMPERATURE_24H_MIN: "Celsius",
            MeasurementType.TEMPERATURE_24H_MAX: "Celsius",
            MeasurementType.HUMIDITY: "percentage",
            MeasurementType.HUMIDITY_24H: "percentage",
            MeasurementType.HUMIDITY_24H_MIN: "percentage",
            MeasurementType.HUMIDITY_24H_MAX: "percentage",
            MeasurementType.WIND_STRENGTH: "km/h",
            MeasurementType.GUST_STRENGTH: "km/h",
            MeasurementType.WIND_STRENGTH_24H: "km/h",
            MeasurementType.GUST_STRENGTH_24H: "km/h",
            MeasurementType.GUST_STRENGTH_24H_MAX: "km/h",
            MeasurementType.WIND_ANGLE: "degrees",
            MeasurementType.GUST_ANGLE: "degrees",
            MeasurementType.WIND_ANGLE_24H: "degrees",
            MeasurementType.GUST_ANGLE_24H: "degrees",
            MeasurementType.RAIN_60MIN: "mm",
            MeasurementType.RAIN_24H: "mm",
            MeasurementType.RAIN_LIVE: "mm",
            MeasurementType.SNOW_24H: "mm",
            MeasurementType.CLOUD_COVERAGE_24H: "eights",
            MeasurementType.PRECIPITATION_TYPE: "code",
            MeasurementType.SUN_24H: "hours",
        }
        return units.get(measurement_type, "Unknown")
    
    @staticmethod
    def is_valid_type(measurement_type: int) -> bool:
        if isinstance(measurement_type, MeasurementType): return True
        if isinstance(measurement_type, int):
            return measurement_type >= 0 and measurement_type <= len(MeasurementType)
        return False