# for deficit calculation
Run for any targetTime e.g. today is 25-05-2025 say I have run script at 2025-05-25 22:10:00 hrs

startTargetTime = 2025-05-25 00:00:00, endTargetTime= 2025-05-25 23:59:00
startDaTargetTime = 2025-05-26 00:00:00 , endDaTargetTime = 2025-05-26 23:59:00

1. It will calculated intraday deficit for today

2. It will calculate DA deficit for tommorrow

3. Fetching Day Ahead and Intraday Revision number for Demand forecast, RE Forecast and schedule based on TargetTime. For schedule dayahead revision for 26 will be created on 25. Make Api call of revision for 25 and 26, 26(DA) revision will be created on 25.

4. Fetching demand forecast data for intraday(startTargetTime, endTargetTime) and dayahead(startDaTargetTime, endDaTargetTime) 

5. Similarly fetching DC, Solar, Wind, Schdule data for both intraday and dayahead.

Loop through all states and perform-

6. Deficit calculation for DA and Intraday using Forecast - (DC + SDL + Wind Forecast + Solar Forecast + others)
concatenate single state deficit to allStateIntradayDeficitData and allStateDayaheadDeficitData

end loop

7. Now get Latest deficit revision No from deficit_revision_metadata table for both DA, Intraday.

8. Checking if calculated deficit changed from existing Deficit present in DB fetched using latest deficit revision no.

9. If deficit is changed, then insert new deficit value with revsion(Rev-old + 1) and also insert date, time, def_type, def_rev_no, forecast_rev_no, sch_rev_no, dc_rev_no, reforecast_rev_no to deficit meta data table

10. Since we are calculating defcit based on combined values of all state, if any state deficit for any block is greter than 50(we have decided) then deficit changed will be true and defecit data will be inserted with increased Revision no. 

11. Similarly if all state deficit data for all block is less than 50 then only we consider deficit value is unchanged , hence no insertion in deficit data table and state deficit meta data table.

