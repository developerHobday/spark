
case class Person(name: String, age: Long)
val df = spark.read.json("/databricks-datasets/samples/people/people.json").as[Person]


case class DeviceIoTData (
    battery_level: Long,
    c02_level: Long,
    cca2: String,
    cca3: String,
    cn: String,
    device_id: Long,
    device_name: String,
    humidity: Long,
    ip: String,
    latitude: Double,
    longitude: Double,
    scale: String,
    temp: Long,
    timestamp: Long,
)
val ds = spark.read.json("/databricks-datasets/iot/iot_devices.json")as[DeviceIoTData]

ds.createOrReplaceTempView("iot_device_data")
/* %sql
select cca3, count(distinct device_id) as device_id
from iot_device_data
group by cca3
order by device_id desc limit 100

*/

# display(ds)
ds.take(10).foreach(
    println(_)
)


val dsTemp = ds.filter( d => d.temp > 25).map( d => (d.temp, d.device_name, d.cca3))
val dsAvgTmp = dsTemp.groupBy($"_3").avg()
