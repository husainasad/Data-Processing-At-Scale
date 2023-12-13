package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // return true
    val rect_coordinates = queryRectangle.split(',')
    val point_Coordinates = pointString.split(',')

    val rect_p1_x = rect_coordinates(0).toDouble
    val rect_p1_y = rect_coordinates(1).toDouble
    val rect_p2_x = rect_coordinates(2).toDouble
    val rect_p2_y = rect_coordinates(3).toDouble

    val point_x = point_Coordinates(0).toDouble
    val point_y = point_Coordinates(1).toDouble

    val rect_x_min = math.min(rect_p1_x, rect_p2_x)
    val rect_x_max = math.max(rect_p1_x, rect_p2_x)
    val rect_y_min = math.min(rect_p1_y, rect_p2_y)
    val rect_y_max = math.max(rect_p1_y, rect_p2_y)

    if(point_x>= rect_x_min && point_x<=rect_x_max && point_y>= rect_y_min && point_y<=rect_y_max)
      return true
    return false    
  }

}
