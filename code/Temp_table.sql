-- Create Temp TABLE

CREATE TABLE public.netscout_average_throughput_test AS
SELECT * FROM cc.netscout_parquet_type_averagethroughput
LIMIT 10000;

--schema
--ap_ip_address
--ap_mac_address
--ap_model
--ap_zone
--client_mac_address
--dt
--egress_active_time
--egress_average_throughput
--egress_total_tonnage
--ingress_active_time
--ingress_average_throughput
--ingress_total_tonnage
--time
--vlan
--vlan_id

-- motionstats_view

CREATE MATERIALIZED VIEW public.motionstats_test
AS SELECT apmac, latitude, longitude, geometry, ap_zone, client_mac_address,
(egress_total_tonnage + ingress_total_tonnage) AS total_tonnage, 
(ingress_active_time + egress_active_time) AS total_time,
time AS timestampID
FROM public.netscout_average_throughput_test as a, cc.gis_final as b
WHERE a.ap_mac_address = b.apmac


--materialview with math

CREATE MATERIALIZED VIEW public.motionstats_test
AS SELECT apmac, latitude, longitude, geometry, ap_zone, client_mac_address,
(egress_total_tonnage + ingress_total_tonnage)/1000 AS total_tonnage_in_kb, 
(ingress_active_time + egress_active_time)/1000 AS total_time_in_seconds,
time AS timestampID, caldate, calhour, dt, vlan_id, vlan
FROM public.netscout_average_throughput_test as a, cc.gis_final as b, cc.date_date_table_parquet as c
WHERE a.ap_mac_address = b.apmac
AND c.timestampid = a.time

--materialized view with redshift geometry
CREATE MATERIALIZED VIEW public.motionstats_test
AS SELECT apmac, latitude, longitude, ap_zone, client_mac_address,
(egress_total_tonnage + ingress_total_tonnage)/1000 AS total_tonnage_in_kb, 
(ingress_active_time + egress_active_time)/1000 AS total_time_in_seconds,
time AS timestampID, caldate, calhour, dt, vlan_id, vlan,
ST_SetSRID(ST_Point(longitude,latitude),4326) as geometry
FROM public.netscout_average_throughput_test as a, cc.gis_final as b, cc.date_date_table_parquet as c
WHERE a.ap_mac_address = b.apmac
AND c.timestampid = a.time


--MakePoint instead of Point
CREATE MATERIALIZED VIEW public.motionstats_test
AS SELECT apmac, latitude, longitude, geometry, ap_zone, client_mac_address,
(egress_total_tonnage + ingress_total_tonnage)/1000 AS total_tonnage_in_kb, 
(ingress_active_time + egress_active_time)/1000 AS total_time_in_seconds,
time AS timestampID, caldate, calhour, dt, vlan_id, vlan,
ST_SetSRID(ST_MakePoint(longitude,latitude),4326) as st_geometry
FROM public.netscout_average_throughput_test as a, cc.gis_final as b, cc.date_date_table_parquet as c
WHERE a.ap_mac_address = b.apmac
AND c.timestampid = a.time
