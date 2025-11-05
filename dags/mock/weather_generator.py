from abc import ABC, abstractmethod

class AbstractMockGenerator(ABC):
    @abstractmethod
    def generate_label(self, row):
        pass


class WeatherMockGenerator(AbstractMockGenerator):
    def generate_label(self, row):
        rh = row.get("relative_humidity_2m (%)", None)
        cc_low = row.get("cloud_cover_low (%)", None)
        cc_total = row.get("cloud_cover (%)", None)
        ws = row.get("wind_speed_10m (km/h)", None)
        precip = row.get("precipitation (mm)", None)
        is_day = row.get("is_day ()", 1)

        # Fog
        if rh and cc_low and ws is not None:
            if rh > 90 and cc_low > 80 and ws < 2:
                return "Fog"

        # Stormy
        if ws and precip is not None:
            if ws > 12 and precip > 2:
                return "Stormy"

        # Sandstorms
        if ws and precip is not None and rh is not None:
            if ws > 10 and precip < 0.1 and rh < 40:
                return "Sandstorms"

        # Windy
        if ws and precip is not None:
            if 6 <= ws <= 12 and precip < 1:
                return "Windy"

        # Cloudy
        if cc_total and precip is not None:
            if cc_total > 70 and precip < 1:
                return "Cloudy"

        # Sunny
        if cc_total is not None and is_day is not None:
            if cc_total < 30 and is_day == 1:
                return "Sunny"

        return "Cloudy"
