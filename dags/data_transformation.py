import pandas as pd
from abc import ABC, abstractmethod


class AbstractDataAlign(ABC):
    @abstractmethod
    def transform_delivery_dataset(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def transform_amazon_dataset(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def align(self) -> pd.DataFrame:
        pass


class DataAligner(AbstractDataAlign):  # ✅ Fixed class name
    def __init__(self, enriched_df: pd.DataFrame, amazon_df: pd.DataFrame):
        self.enriched_df = enriched_df
        self.amazon_df = amazon_df

    def transform_delivery_dataset(self) -> pd.DataFrame:
        """Reshape the enriched delivery dataset into common schema"""
        df1 = self.enriched_df.copy()
        
        # ✅ Check if dataframe is empty
        if df1.empty:
            print("⚠️ Warning: Enriched dataframe is empty!")
            return pd.DataFrame()
        
        print(f"📊 Transforming delivery dataset: {df1.shape}")
        print(f"   Columns: {df1.columns.tolist()}")
        
        # ✅ Use accept_time column (already exists from enrichment)
        # Make sure it's datetime type
        df1["accept_time"] = pd.to_datetime(df1["accept_time"], errors="coerce")
        df1["delivery_time"] = pd.to_datetime(df1["delivery_time"], errors="coerce")
        
        pickup_datetime1 = df1["accept_time"]  # ✅ Use accept_time, not "time"
        
        df1_reshaped = pd.DataFrame({
            "order_id": df1["order_id"],
            "Date": pickup_datetime1.dt.date,
            "pickup_time": pickup_datetime1,
            "delivery_time": df1["delivery_time"],
            "pickup_lat": df1["accept_gps_lat"].fillna(df1["lat"]),
            "pickup_lng": df1["accept_gps_lng"].fillna(df1["lng"]),
            "drop_lat": df1["delivery_gps_lat"],
            "drop_lng": df1["delivery_gps_lng"],
            "weather": df1.get("Weather_Label", None),  # ✅ Use .get() for safety
            "traffic": df1.get("Traffic_Label", None)   # ✅ Use .get() for safety
        })
        
        # ✅ Calculate ETA (should already exist from enrichment, but recalculate for consistency)
        df1_reshaped["ETA_target"] = (
            (df1_reshaped["delivery_time"] - df1_reshaped["pickup_time"])
            .dt.total_seconds() / 60
        )
        
        print(f"✅ Transformed delivery dataset: {df1_reshaped.shape}")
        return df1_reshaped

    def transform_amazon_dataset(self) -> pd.DataFrame:
        """Reshape the amazon dataset into common schema"""
        df2 = self.amazon_df.copy()
        
        if df2.empty:
            print("⚠️ Warning: Amazon dataframe is empty!")
            return pd.DataFrame()
        
        print(f"📊 Transforming Amazon dataset: {df2.shape}")
        print(f"   Columns: {df2.columns.tolist()}")
        
        # ✅ Combine Order_Date and Order_Time
        pickup_datetime = pd.to_datetime(
            df2["Order_Date"].astype(str) + " " + df2["Order_Time"].fillna("00:00:00"),
            errors="coerce"
        )
        
        # ✅ Calculate delivery time
        delivery_datetime = pickup_datetime + pd.to_timedelta(
            df2["Delivery_Time"], 
            unit="m"
        )
        
        df2_reshaped = pd.DataFrame({
            "order_id": df2["Order_ID"],
            "Date": df2["Order_Date"],
            "pickup_time": pickup_datetime,
            "delivery_time": delivery_datetime,
            "pickup_lat": df2["Store_Latitude"],
            "pickup_lng": df2["Store_Longitude"],
            "drop_lat": df2["Drop_Latitude"],
            "drop_lng": df2["Drop_Longitude"],
            "traffic": df2["Traffic"],
            "weather": None  # ✅ Amazon data doesn't have weather
        })
        
        df2_reshaped["ETA_target"] = (
            (df2_reshaped["delivery_time"] - df2_reshaped["pickup_time"])
            .dt.total_seconds() / 60.0
        )
        
        print(f"✅ Transformed Amazon dataset: {df2_reshaped.shape}")
        return df2_reshaped

    def align(self) -> pd.DataFrame:
        """Align both datasets into a single final dataset"""
        print("\n🔄 Starting alignment process...")
        
        df1 = self.transform_delivery_dataset()
        df2 = self.transform_amazon_dataset()
        
        print(f"   Delivery rows: {len(df1)}")
        print(f"   Amazon rows: {len(df2)}")
        
        # ✅ Handle case where one or both are empty
        if df1.empty and df2.empty:
            print("⚠️ Both datasets are empty!")
            return pd.DataFrame()
        elif df1.empty:
            print("⚠️ Delivery dataset is empty, returning Amazon only")
            return df2
        elif df2.empty:
            print("⚠️ Amazon dataset is empty, returning Delivery only")
            return df1
        
        final_df = pd.concat([df1, df2], ignore_index=True)
        
        print(f"✅ Final aligned dataset: {final_df.shape}")
        print(f"   Columns: {final_df.columns.tolist()}")
        
        return final_df