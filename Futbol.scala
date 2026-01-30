
import cats.effect.{IO, IOApp}
import cats.implicits._
import doobie._
import doobie.implicits._
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv._
import fs2.data.csv.generic.semiauto._

case class futbol1 (
                  ID: Int,
                  Equipo_Local: String,
                  Equipo_Visitante: String,
                  Fecha_Partido: Option[String],
                  Estadio: String,
                  Goles_Local: Int,
                  Goles_Visitante: Int,
                  Partido_Jugado: String

                  )

given csvDecoder: CsvRowDecoder[futbol1, String] = deriveCsvRowDecoder[futbol1]


object Estadistica:

  def Goles_T (datos: List[Int]): Int  =
    if datos.isEmpty then 0
    else datos.sum

  def Goles_Promedio (datos: List[Int]): Double  =
    if datos.isEmpty then 0.0
    else datos.sum/datos.length.toDouble

object BaseDeDatos{
   val xa = Transactor.fromDriverManager[IO](
    driver = "oracle.jdbc.OracleDriver",
    url = "jdbc:oracle:thin:@//localhost:1521/XEPDB1",
    user = "futbol",
    password = "futbol",
    logHandler = None
  )

  def createTable: ConnectionIO[Int] =
    sql"""
       CREATE TABLE FUTBAL (
         ID NUMBER PRIMARY KEY,
         EQUIPO_LOCAL VARCHAR(100),
         EQUIPO_VISITANTE VARCHAR(100),
         FECHA_PARTIDO DATE,
         ESTADIO VARCHAR(100),
         GOLES_LOCAL NUMBER,
         GOLES_VISITANTE NUMBER,
         PARTIDO_JUGADO VARCHAR(100)
       )
     """.update.run

   def insertar(m: futbol1): ConnectionIO [Int] =
     sql"""
          INSERT INTO FUTBAL (ID, Equipo_Local, Equipo_Visitante, Fecha_Partido, Estadio, Goles_Local,Goles_Visitante, Partido_Jugado)
          VALUES (${m.ID}, ${m.Equipo_Local}, ${m.Equipo_Visitante}, ${m.Fecha_Partido},${m.Estadio},${m.Goles_Local},${m.Goles_Visitante},${m.Partido_Jugado})
        """.update.run

   def listar : ConnectionIO[List[futbol1]]=
     sql"SELECT id, equipo_local, equipo_visitante,fecha_partido,estadio, goles_local,goles_visitante, partido_jugado FROM Futbal"
       .query[futbol1]
       .to[List]
}
object Futbol extends IOApp.Simple {
  val filePath = Path("C:\\Users\\andx\\IdeaProjects\\exp2\\src\\main\\resources\\data\\futbol.csv"
  )
  val run: IO[Unit] = {
    val lecturaCSV: IO[List[futbol1]] =
      Files[IO]
        .readAll(filePath)
        .through(text.utf8.decode)
        .through(decodeUsingHeaders[futbol1](','))
        .compile
        .toList

    for {
      _ <- IO.println("--- 1. Leyendo Archivo CSV ---")
      datos <- lecturaCSV
      _ <- IO.println(s"--> Datos en memoria: ${datos.length}")

      _ <- IO.println("\n--- 2. Calculando Estadísticas ---")
      glocal = datos.map(_.Goles_Local)
      gvisitante = datos.map(_.Goles_Visitante)
      gtt = Estadistica.Goles_T(glocal)
      gtp = Estadistica.Goles_Promedio(glocal)
      gvt = Estadistica.Goles_T(gvisitante)
      gvp = Estadistica.Goles_Promedio(gvisitante)

      _ <- IO.println(f"  Goles locales totales: $gtt")
      _ <- IO.println(f"  Goles locales promedio:   $gtp")
      _ <- IO.println(f"  Goles visitante totales:   $gvt")
      _ <- IO.println(f"  Goles visitante promedio:   $gvp")

      _ <- IO.println("\n--- 3. Insertando en Oracle ---")
      _ <- BaseDeDatos.createTable.transact(BaseDeDatos.xa)
      filas <- datos.traverse(d => BaseDeDatos.insertar(d).transact(BaseDeDatos.xa))
      _ <- IO.println(s" ÉXITO: Se guardaron ${filas.size} registros.")

      // --- 4. VERIFICAR QUE SE GUARDARON (LO NUEVO) ---
      _ <- IO.println("\n--- 4. Consultando Registros desde Oracle ---")
      registrosBD <- BaseDeDatos.listar.transact(BaseDeDatos.xa)

      _ <- IO(registrosBD.foreach(r => println(s"  -> ${r.Goles_Local} (${r.Goles_Visitante}): partidos ${r.Partido_Jugado}")))

    } yield ()
  }

}
