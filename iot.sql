

-- Clean -- 
/*
drop nulls / nas
range values
not numeric for numeric fields
not in range for enum fields
conflicting data colummns - same device_id, different device_name
regex - e.g. for IP
PII
standardization of metrics - between scales
check if temperature is significanty different from others that month.


Transform
- Integrate with star schema 

*/

-- Analytics -- 
/*

For a device / country, see the humidity / temperature / battery level by time
Does device latitude change?
Get  daily temperature average / max / min over the past 5 years


*/

-- Number of devices by country
select cca3, count(distinct device_id) as device_id
from iot_device_data
group by cca3
order by device_id desc limit 100