import com.opencagedata.geocoder.OpenCageClient
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAllConfigMap, ConfigMap}

object UnitTest {
  def main(args: Array[String]): Unit = {
    (new RunTest).execute(configMap = ConfigMap.apply(("key", args(0))))
  }
}

class RunTest extends AnyFlatSpec with BeforeAndAfterAllConfigMap  {

  "GeoInfo" should "return geo location" in {
    assert(GeoInfo.getGeoPos("KZ, Almaty", geoClient) === GeoPos(43.236393.toFloat, 76.945724.toFloat))
  }
  it should "return GeoPos(0, 0) if address not found" in {
    assert(GeoInfo.getGeoPos("", geoClient) === GeoPos(0.toFloat, 0.toFloat))
  }


  var geoClient: OpenCageClient = null
  override def beforeAll(configMap: ConfigMap): Unit = {
    val fKey = configMap.get("key").getOrElse().toString
    geoClient = new OpenCageClient(fKey)
  }

  override def afterAll(configMap: ConfigMap): Unit = {
    geoClient.close()
  }
}
