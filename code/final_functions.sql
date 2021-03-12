CREATE MATERIALIZED VIEW cc.motionstats_base_vw
auto refresh yes
AS SELECT apmac, latitude, longitude, geometry, ap_zone, client_mac_address,
(egress_total_tonnage + ingress_total_tonnage)/1000 AS total_tonnage_in_kb, 
(ingress_active_time + egress_active_time)/1000 AS total_time_in_seconds,
time AS timestampID, caldate As Date, calhour, dt, vlan_id, vlan,
ST_SetSRID(ST_MakePoint(longitude,latitude),4326) as st_geometry
FROM cc.netscout_parquet_type_averagethroughput as a, cc.gis_final as b, cc.date_date_table_parquet as c
WHERE lower(a.ap_mac_address) = lower(b.apmac)
AND c.timestampid = a.time


create view cc.motionstats_calculated_vw
AS SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp, 
latitude as origin_latitude, longitude as origin_longitude,
LAG(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp,
LAG(latitude) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_latitude,
LAG(longitude) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_longitude
FROM cc.motionstats_base_vw