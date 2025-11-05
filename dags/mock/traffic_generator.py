from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from datetime import datetime


class BaseMockGenerator(ABC):
    @abstractmethod
    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


class TrafficMockGenerator(BaseMockGenerator):
    def __init__(self):
        self.traffic_labels = ["Low", "Medium", "High", "Jam"]
      
    def _map_traffic(self, time_val):
        """
        Rules based on delivery_time (supports both datetime and string).
        """
        if isinstance(time_val, str):
            try:
                t = datetime.strptime(time_val, "%m-%d %H:%M:%S").time()
            except Exception:
                return "Medium"
        elif isinstance(time_val, pd.Timestamp):
            t = time_val.time()
        else:
            return "Medium"

        if datetime.strptime("07:00:00", "%H:%M:%S").time() <= t <= datetime.strptime("09:00:00", "%H:%M:%S").time():
            return "High"
        elif datetime.strptime("17:00:00", "%H:%M:%S").time() <= t <= datetime.strptime("19:00:00", "%H:%M:%S").time():
            return "Jam"
        elif t >= datetime.strptime("22:00:00", "%H:%M:%S").time() or t <= datetime.strptime("05:00:00", "%H:%M:%S").time():
            return "Low"
        else:
            return "Medium"

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Traffic_Label"] = df["delivery_time"].apply(self._map_traffic)
        return df
