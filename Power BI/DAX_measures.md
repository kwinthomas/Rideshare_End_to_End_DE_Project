# DAX measures — rideshare star schema
---

## Volume

```dax
Total Rides = COUNTROWS ( fct_rides )
```

```dax
Completed Rides = CALCULATE ( [Total Rides], fct_rides[is_completed] = TRUE () )
```

```dax
Cancelled Rides = CALCULATE ( [Total Rides], fct_rides[is_cancelled] = TRUE () )
```

```dax
Completion Rate = DIVIDE ( [Completed Rides], [Total Rides] )
```

```dax
Cancellation Rate = DIVIDE ( [Cancelled Rides], [Total Rides] )
```

---

## Revenue

```dax
Total Revenue = SUM ( fct_rides[total_fare] )
```

```dax
Completed Revenue = CALCULATE ( [Total Revenue], fct_rides[is_completed] = TRUE () )
```

```dax
Total Tips = SUM ( fct_rides[tip_amount] )
```

```dax
Net Subtotal = SUM ( fct_rides[subtotal] )
```

```dax
Avg Fare = AVERAGE ( fct_rides[total_fare] )
```

```dax
Tip Rate = DIVIDE ( [Total Tips], [Net Subtotal] )
```

---

## Surge and distance

```dax
Surged Rides = CALCULATE ( [Total Rides], fct_rides[is_surged] = TRUE () )
```

```dax
Surge Share = DIVIDE ( [Surged Rides], [Total Rides] )
```

```dax
Avg Surge Multiplier = CALCULATE ( AVERAGE ( fct_rides[surge_multiplier] ), fct_rides[is_surged] = TRUE () )
```

```dax
Total Miles = SUM ( fct_rides[distance_miles] )
```

```dax
Total Hours = DIVIDE ( SUM ( fct_rides[duration_minutes] ), 60 )
```

```dax
Intercity Share = DIVIDE ( CALCULATE ( [Total Rides], fct_rides[is_intercity] = TRUE () ), [Total Rides] )
```

---

## Ratings

```dax
Rated Rides = CALCULATE ( [Total Rides], fct_rides[was_rated] = TRUE () )
```

```dax
Rating Response Rate = DIVIDE ( [Rated Rides], [Completed Rides] )
```

```dax
Avg Passenger Rating = AVERAGE ( fct_rides[passenger_rating_given] )
```

---

## Time intelligence

```dax
Revenue MTD = TOTALMTD ( [Total Revenue], dim_date[calendar_date] )
```

```dax
Revenue Prior Month = CALCULATE ( [Total Revenue], DATEADD ( dim_date[calendar_date], -1, MONTH ) )
```

```dax
Revenue MoM % =
VAR Current = [Total Revenue]
VAR Prior = [Revenue Prior Month]
RETURN
    IF ( NOT ISBLANK ( Prior ), DIVIDE ( Current - Prior, Prior ) )
```

```dax
Rides Rolling 7 Days = CALCULATE ( [Total Rides], DATESINPERIOD ( dim_date[calendar_date], MAX ( dim_date[calendar_date] ), -7, DAY ) )
```

---

## Streaming freshness and data quality

```dax
Last Ride Received = MAX ( fct_rides[pickup_timestamp] )
```

```dax
Minutes Since Last Ride = DATEDIFF ( [Last Ride Received], NOW (), MINUTE )
```

```dax
Rides Last 24 Hours = CALCULATE ( [Total Rides], FILTER ( ALL ( fct_rides ), fct_rides[pickup_timestamp] >= NOW () - 1 ) )
```

```dax
Streaming Rides = CALCULATE ( [Total Rides], fct_rides[_source_system] = "eventhub" )
```

```dax
Backfilled Rides = CALCULATE ( [Total Rides], fct_rides[_source_system] = "bulk_file" )
```