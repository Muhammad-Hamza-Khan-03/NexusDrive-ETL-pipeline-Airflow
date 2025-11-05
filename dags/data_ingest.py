import os
import pandas as pd
from abc import ABC, abstractmethod
from mock.weather_generator import WeatherMockGenerator
from mock.traffic_generator import TrafficMockGenerator


class AbstractDataIngest(ABC):
    @abstractmethod
    def load_data(self):
        pass

    @abstractmethod
    def enrich_with_weather(self):
        pass

    @abstractmethod
    def enrich_with_traffic_and_vehicles(self):
        pass


class DataIngest(AbstractDataIngest):
    def __init__(self, delivery_file: str, weather_file: str):
        self.delivery_file = delivery_file
        self.weather_file = weather_file
        self.delivery_df = None
        self.weather_df = None
        self.load_data()

    def load_data(self):
        # ✅ File existence checks
        if not os.path.exists(self.delivery_file):
            raise FileNotFoundError(f"❌ Delivery file not found: {self.delivery_file}")
        if not os.path.exists(self.weather_file):
            raise FileNotFoundError(f"❌ Weather file not found: {self.weather_file}")

        # ✅ Load CSV
        self.delivery_df = pd.read_csv(self.delivery_file)
        self.weather_df = pd.read_csv(self.weather_file)

        print(f"✅ Loaded delivery data: {self.delivery_df.shape}")
        print(f"   Columns: {self.delivery_df.columns.tolist()}")
        print(f"✅ Loaded weather data: {self.weather_df.shape}")
        print(f"   Columns: {self.weather_df.columns.tolist()}")

        # ✅ Normalize columns (fix spaces, casing)
        self.delivery_df.columns = self.delivery_df.columns.str.strip().str.lower()
        self.weather_df.columns = self.weather_df.columns.str.strip().str.lower()

        # ✅ Detect and convert time columns
        if "accept_time" in self.delivery_df.columns:
            self.delivery_df["accept_time"] = pd.to_datetime(
                self.delivery_df["accept_time"], errors="coerce"
            )
        else:
            raise KeyError("❌ Missing 'accept_time' in delivery data.")

        if "delivery_time" in self.delivery_df.columns:
            self.delivery_df["delivery_time"] = pd.to_datetime(
                self.delivery_df["delivery_time"], errors="coerce"
            )

        # Detect weather timestamp column automatically
        time_candidates = ["time_weather", "timestamp", "datetime", "time", "weather_time"]
        weather_time_col = next((c for c in time_candidates if c in self.weather_df.columns), None)

        if not weather_time_col:
            raise KeyError("❌ No timestamp column found in weather file.")

        self.weather_df[weather_time_col] = pd.to_datetime(
            self.weather_df[weather_time_col], errors="coerce"
        )

        # ✅ Quality checks
        print(f"\n📊 Data Quality Check:")
        print(f"   Nulls in delivery 'accept_time': {self.delivery_df['accept_time'].isnull().sum()}")
        if "delivery_time" in self.delivery_df.columns:
            print(f"   Nulls in delivery 'delivery_time': {self.delivery_df['delivery_time'].isnull().sum()}")
        print(f"   Nulls in weather '{weather_time_col}': {self.weather_df[weather_time_col].isnull().sum()}")

        print(f"\n📋 Sample accept_time values:")
        print(self.delivery_df["accept_time"].head())
        print(f"\n📋 Sample weather time values:")
        print(self.weather_df[weather_time_col].head())

        # ✅ Save detected column for later use
        self.weather_time_col = weather_time_col

    def enrich_with_weather(self) -> pd.DataFrame:
        """Align delivery times with nearest earlier weather hour."""
        delivery_clean = self.delivery_df.dropna(subset=["accept_time"]).copy()
        weather_clean = self.weather_df.dropna(subset=[self.weather_time_col]).copy()

        print(f"\n🔄 Weather Merge Debug:")
        print(f"   Delivery records: {len(delivery_clean)}")
        print(f"   Weather records: {len(weather_clean)}")
        print(f"   Delivery time range: {delivery_clean['accept_time'].min()} to {delivery_clean['accept_time'].max()}")
        print(f"   Weather time range: {weather_clean[self.weather_time_col].min()} to {weather_clean[self.weather_time_col].max()}")

        delivery_clean = delivery_clean.sort_values("accept_time")
        weather_clean = weather_clean.sort_values(self.weather_time_col)

        # ✅ Merge on nearest earlier timestamp (no suffix)
        merged = pd.merge_asof(
            delivery_clean,
            weather_clean,
            left_on="accept_time",
            right_on=self.weather_time_col,
            direction="backward"
        )

        # ✅ Rename the merged weather timestamp column consistently
        if self.weather_time_col != "time_weather":
            merged.rename(columns={self.weather_time_col: "time_weather"}, inplace=True)

        print(f"   ✅ Merged records: {len(merged)}")
        print(f"   ✅ Records with weather timestamps: {merged['time_weather'].notna().sum()}")

        # ✅ Generate weather label
        generator = WeatherMockGenerator()
        merged["Weather_Label"] = merged.apply(generator.generate_label, axis=1)

        self.delivery_df = merged
        return self.delivery_df

    def enrich_with_traffic_and_vehicles(self) -> pd.DataFrame:
        """Add traffic and vehicle info using TrafficMockGenerator."""
        generator = TrafficMockGenerator()
        self.delivery_df = generator.generate(self.delivery_df)
        return self.delivery_df
