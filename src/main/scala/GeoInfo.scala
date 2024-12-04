import com.opencagedata.geocoder._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object GeoInfo {
  def getGeoPos(aAddress: String, geoClient: OpenCageClient): GeoPos = {

    val params = OpenCageClientParams(
      limit = Some(1))

    try {
      val responseFuture = geoClient.forwardGeocode(aAddress, params)
      val response = Await.result(responseFuture, 5.seconds)
      response.results.head.geometry.map(part => {
        GeoPos(part.lat, part.lng)
      }).head
    }
    catch {
      case _: Throwable => println("No address found for \"" + aAddress + "\"")
        GeoPos(0, 0)
    }
  }
}

case class GeoPos(lat: Float, lng: Float);
