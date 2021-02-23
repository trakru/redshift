--motionstats_test schema

--ap_zone
--apmac
--caldate
--calhour
--client_mac_address
--dt
--geometry
--latitude
--longitude
--timestampid
--total_time_in_seconds
--total_tonnage_in_kb
--vlan
--vlan_id

--final required schema
--client_mac_address
--apmac
--timestampID
--vlan_ID
--vlan
--total_tonnage_in_kb
--total_time_in_seconds
--origin_geometry
--destination_geometry

--spatial calculations
--distance
--EastDiff
--NorthDiff

-- LOGIC (LAG)
-- IF timestampID(2)-6 

--AND client_mac_address(2) = client_mac_address(1)
--THEN origin_geometry=geometry(1), destination_geometry=geometry(2)
--ELSE origin_geometry=geometry(1), destination_geometry=geometry(1)
--order BY client_mac_address, timestampID


--LOGIC (LAG)
--IF timestampID(2) - timestampID(1) < 3 
--AND client_mac_address(2) = client_mac_address(1)
--THEN origin_geometry=geometry(1), destination_geometry=geometry(2)
--ELSE origin_geometry=geometry(1), destination_geometry=geometry(1)
--order BY client_mac_address, timestampID

SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp,
LEAD(timestampID,1) OVER(ORDER BY client_mac_address, timestampID) as destination_timestamp
FROM public.motionstats_test

SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp, geometry as origin_geometry,
LEAD(timestampID,1) OVER(ORDER BY client_mac_address, timestampID) as destination_timestamp, 
LEAD(geometry,1) OVER(ORDER BY client_mac_address, timestampID) as destination_geometry
FROM public.motionstats_test
LIMIT 10;

--same session
--new session

select client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp,
case
when lag(trunc(timestampID)) over(partition by client_mac_address order by timestampID asc) = trunc(timestampID)
then 

SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp,
LAG(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp
FROM public.motionstats_test

create public.motionstats_test2 as
SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp,
LEAD(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp,
(destination_timestamp - origin_timestamp) as time_diff
FROM public.motionstats_test


create view public.motionstats_test2 as
SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp, st_geometry as origin_geometry,
LEAD(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp,
LEAD(st_geometry) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_geometry,
ST_DistanceSphere(origin_geometry, destination_geometry) as total_distance
FROM public.motionstats_test

create view public.motionstats_test2 as
SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp, st_geometry,
LEAD(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp,
LEAD(st_geometry) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_geometry,
ST_DistanceSphere(st_geometry, destination_geometry) as total_distance
FROM public.motionstats_test

create view public.motionstats_test2 as
SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp, st_geometry,
LAG(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp,
LAG(st_geometry) as destination_geometry,
ST_DistanceSphere(st_geometry, destination_geometry) as total_distance
FROM public.motionstats_test

POINT (-75.157853 39.945803)
POINT (-75.143234 39.723403)
SELECT ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326);
SELECT ST_GeomFromText('POINT (-75.157853 39.945803)',4326);


--working version
create view public.motionstats_test2 as
SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp, geometry as origin_geometry,
LAG(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp,
LAG(geometry) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_geometry
--ST_DistanceSphere(st_geometry, destination_geometry) as total_distance
FROM public.motionstats_test

create view public.motionstats_test2 as
SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp, geometry as origin_geometry,
LAG(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp,
LAG(geometry) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_geometry
--ST_DistanceSphere(st_geometry, destination_geometry) as total_distance
FROM public.motionstats_test

create view public.motionstats_test2 as
SELECT client_mac_address, apmac, vlan_ID, timestampID as origin_timestamp, 
latitude as origin_latitude, longitude as origin_longitude,
LAG(timestampID) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_timestamp,
LAG(latitude) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_latitude,
LAG(longitude) OVER(PARTITION BY client_mac_address ORDER BY timestampID) as destination_longitude
--ST_DistanceSphere(st_geometry, destination_geometry) as total_distance
FROM public.motionstats_test



