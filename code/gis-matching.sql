select count(distinct ap_mac_address) 
from cc.netscout_parquet_type_averagethroughput as a, cc.gis_final as b
where lower(a.ap_mac_address) = lower(b.apmac)