import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.bson.json.JsonParseException
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, MongoDatabase, Observable, Observer}

object SparkKafkaConnect{

  val mongoClient:MongoClient = MongoClient()
  val database:MongoDatabase = mongoClient.getDatabase("Twitter")
  val collection:MongoCollection[Document] = database.getCollection("raw_data1")
  if (mongoClient==null) print("Unable to connect to database")


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("TweetStream")
      .master("local[*]")
      .getOrCreate()

    val ds = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","15_04_tweets")
      .option("startingOffsets","earliest")
      .load()

    val selectds = ds.selectExpr("CAST(value AS STRING)")

    val consumer = new ForeachWriter[Row]
    {
      def open(partitionId: Long, version: Long): Boolean = {
        true
      }

      def process(record: Row): Unit = {
        // Write string to connection
        try{
          val (output_processed) = extractData(record(0).toString)
          if(output_processed!=() && output_processed!=null)
          {
            println(output_processed)
            val doc:Document = Document(output_processed)
            insertIntoDB(doc,collection)
          }
        }catch {
          case o:ArrayIndexOutOfBoundsException => println("Text or Country is empty , or may be filtered out")
          case j:JsonParseException => println("Data is not in good Format")
          case _:Throwable => println("Error encontered here : "+record(0).toString)

        }

      }
      def close(errorOrNull: Throwable): Unit = {
        println("Some error or NUll tweet..")
      }
    }

    val writedf = selectds.writeStream
      .foreach(consumer)
      .start()
    writedf.awaitTermination()
  }

  /** Function for extracting tweets data from Json file <- from KafkaTopic */
  def  extractData(line:String): (String) =
  {
  val parser_ = new JSONParser()
  val object_ = (parser_).parse(line)
  val tweet_object = object_.asInstanceOf[JSONObject]
  try {

    val created_at = tweet_object.get("created_at").asInstanceOf[String]
    val text = tweet_object.get("text").asInstanceOf[String]
    val stopWords = Set("a","an","is","was","the","to","RT","of","in","and","are","on","de","for","","The","that","this","la","el","en","ourselves", "hers", "between", "yourself", "but", "again", "there", "about", "once", "during", "out",
      "very", "having", "with", "they", "own", "be", "some", "for", "do", "its", "yours", "such", "into", "of", "most", "itself",
      "other", "off", "s", "am", "or", "who", "as", "from", "him", "each", "the", "themselves", "until", "below", "we", "these",
      "your", "his", "through", "don", "nor", "me", "were", "her", "more", "himself", "this", "down", "should", "our", "their", "while", "above",
      "both", "up", "to", "ours", "had", "she", "all", "no", "when", "at", "any", "before", "them", "same", "and", "been", "have", "in", "will",
      "does", "yourselves", "then", "that", "because", "what", "over", "why", "so", "can", "did", "not", "now", "under", "he", "you", "herself", "has", "just",
      "where", "too", "only", "myself", "which", "those", "i", "after", "few", "whom", "t", "being", "if", "theirs", "my", "against", "by", "doing", "it",
      "how", "further", "here", "than", "", "eu", "gosto", "der" ,"RT")

    val text_emoji_filtered = text.replaceAll("[-+.^!?:=;%,&\"(\\u00a9|\\u00ae|[\\u2000-\\u3300]|\\ud83c[\\ud000-\\udfff]|\\ud83d[\\ud000-\\udfff]|\\ud83e[\\ud000-\\udfff])]","")

    val filteredText = text_emoji_filtered.toLowerCase.split(" ").distinct.filterNot(x => stopWords.contains(x)).filter(x => x.matches("[a-zA-Z]*$")).filter(x => x.length >2).mkString(" ")


    /** Use this to collect top 100 Words */

    /** Find date format of mongoDB (yyyy-mm-dd) */
//    print("creation time " + created_at)
//    debugging point
    val fields = created_at.split(' ')

    var month = fields(1)
    val date = fields(2)
    val year = fields(5)


      try{
        month match {
          case "Jan" => month = "01"
          case "Feb" => month = "02"
          case "Mar" => month = "03"
          case "Apr" => month = "04"
          case "May" => month = "05"
          case "Jun" => month = "06"
          case "Jul" => month = "07"
          case "Aug" => month = "08"
          case "Sep" => month = "09"
          case "Oct" => month = "10"
          case "Nov" => month = "11"
          case "Dec" => month = "12"
        }
      }
      catch
        {
          case e:Exception => print("month not found")
        }

    val created = s"""$year-$month-$date"""


        println(s" to be printed : $year-$month-$date")

        var user_location = tweet_object.get("user").asInstanceOf[JSONObject].get("location").asInstanceOf[String]
    var location_filtered = cleaningLocation(user_location)
    if(location_filtered == null && filteredText!=null) return (null)
        return (s"""{created : \"$created\" , location : \"$location_filtered\" , text: \"$filteredText\"}""")

    }catch {
    case nullptr: NullPointerException => println("location data is missing ")
      return (null)
     }

  }

  def insertIntoDB(doc:Document,collection: MongoCollection[Document]): Unit =
  {

    val observable:Observable[Completed] = collection.insertOne(doc)
    observable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println("Inserted")
      override def onError(e: Throwable): Unit = println("Failed")
      override def onComplete(): Unit = println("Completed")
    })
  }

  def cleaningLocation(line:String): String =
  {

    val fields = line.split(",")

    /** creating a Map and Set of countries */
    val countriesSet = Set("Afghanistan",
            "Åland Islands",
            "Albania",
            "Algeria",
            "American Samoa",
            "AndorrA",
            "Angola",
            "Anguilla",
            "Antarctica",
            "Antigua And Barbuda",
            "Argentina",
            "Armenia",
            "Aruba",
            "Australia",
            "Austria",
            "Azerbaijan",
            "Bahamas",
            "Bahrain",
            "Bangladesh",
            "Barbados",
            "Belarus",
            "Belgium",
            "Belize",
            "Benin",
            "Bermuda",
            "Bhutan",
            "Bolivia",
            "Bosnia And Herzegovina",
            "Botswana",
            "Bouvet Island",
            "Brazil",
            "British Indian Ocean Territory",
            "Brunei Darussalam",
            "Bulgaria",
            "Burkina Faso",
            "Burundi",
            "Cambodia",
            "Cameroon",
            "Canada",
            "Cape Verde",
            "Cayman Islands",
            "Central African Republic",
            "Chad",
            "Chile",
            "China",
            "Christmas Island",
            "Cocos (Keeling) Islands",
            "Colombia",
            "Comoros",
            "Congo",
            "Congo",
            "Cook Islands",
            "Costa Rica",
            "Cote DIvoire",
            "Croatia",
            "Cuba",
            "Cyprus",
            "Czech Republic",
            "Denmark",
            "Djibouti",
            "Dominica",
            "Dominican Republic",
            "Ecuador",
            "Egypt",
            "El Salvador",
            "Equatorial Guinea",
            "Eritrea",
            "Estonia",
            "Ethiopia",
            "Falkland Islands, Malvinas",
            "Faroe Islands",
            "Fiji",
            "Finland",
            "France",
            "French Guiana",
            "French Polynesia",
            "French Southern Territories",
            "Gabon",
            "Gambia",
            "Georgia",
            "Germany",
            "Ghana",
            "Gibraltar",
            "Greece",
            "Greenland",
            "Grenada",
            "Guadeloupe",
            "Guam",
            "Guatemala",
            "Guernsey",
            "Guinea",
            "Guinea-bissau",
            "Guyana",
            "Haiti",
            "Heard Island And Mcdonald Islands",
            "Holy See, Vatican City State",
            "Honduras",
            "Hong Kong",
            "Hungary",
            "Iceland",
            "India",
            "Indonesia",
            "Iran",
            "Iraq",
            "Ireland",
            "Isle Of Man",
            "Israel",
            "Italy",
            "Jamaica",
            "Japan",
            "Jersey",
            "Jordan",
            "Kazakhstan",
            "Kenya",
            "Kiribati",
            "Korea",
            "Korea",
            "Kuwait",
            "Kyrgyzstan",
            "Lao People's Democratic Republic",
            "Latvia",
            "Lebanon",
            "Lesotho",
            "Liberia",
            "Libyan Arab Jamahiriya",
            "Liechtenstein",
            "Lithuania",
            "Luxembourg",
            "Macao",
            "Macedonia",
            "Madagascar",
            "Malawi",
            "Malaysia",
            "Maldives",
            "Mali",
            "Malta",
            "Marshall Islands",
            "Martinique",
            "Mauritania",
            "Mauritius",
            "Mayotte",
            "Mexico",
            "Micronesia",
            "Moldova",
            "Monaco",
            "Mongolia",
            "Montserrat",
            "Morocco",
            "Mozambique",
            "Myanmar",
            "Namibia",
            "Nauru",
            "Nepal",
            "Netherlands",
            "Netherlands Antilles",
            "New Caledonia",
            "New Zealand",
            "Nicaragua",
            "Niger",
            "Nigeria",
            "Niue",
            "Norfolk Island",
            "Northern Mariana Islands",
            "Norway",
            "Oman",
            "Pakistan",
            "Palau",
            "Palestinian Territory",
            "Panama",
            "Papua New Guinea",
            "Paraguay",
            "Peru",
            "Philippines",
            "Pitcairn",
            "Poland",
            "Portugal",
            "Puerto Rico",
            "Qatar",
            "Reunion",
            "Romania",
            "Russian Federation",
            "Rwanda",
            "Saint Helena",
            "Saint Kitts And Nevis",
            "Saint Lucia",
            "Saint Pierre And Miquelon",
            "Saint Vincent And The Grenadines",
            "Samoa",
            "San Marino",
            "Sao Tome And Principe",
            "Saudi Arabia",
            "Senegal",
            "Serbia And Montenegro",
            "Seychelles",
            "Sierra Leone",
            "Singapore",
            "Slovakia",
            "Slovenia",
            "Solomon Islands",
            "Somalia",
            "South Africa",
            "South Georgia And The South Sandwich Islands",
            "Spain",
            "Sri Lanka",
            "Sudan",
            "Suriname",
            "Svalbard And Jan Mayen",
            "Swaziland",
            "Sweden",
            "Switzerland",
            "Syrian Arab Republic",
            "Taiwan",
            "Tajikistan",
            "Tanzania",
            "Thailand",
            "Timor-leste",
            "Togo",
            "Tokelau",
            "Tonga",
            "Trinidad And Tobago",
            "Tunisia",
            "Turkey",
            "Turkmenistan",
            "Turks and Caicos Islands",
            "Tuvalu",
            "Uganda",
            "Ukraine",
            "United Arab Emirates",
            "United Kingdom",
            "United States",
            "United States Minor Outlying Islands",
            "Uruguay",
            "Uzbekistan",
            "Vanuatu",
            "Venezuela",
            "Viet Nam",
            "Virgin Islands, British",
            "Virgin Islands, United States",
            "Wallis and Futuna",
            "Western Sahara",
            "Yemen",
            "Zambia",
            "Zimbabwe")
    val countriesCodeSet = Set("AF","AX","AL","DZ","AS","AD","AO","AI","AQ","AG","AR","AM","AW","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BM","BT","BO","BA","BW","BV","BR","IO","BN","BG","BF","BI","KH","CM","CA","CV","KY","CF","TD","CL","CN","CX","CC","CO","KM","CG","CD","CK","CR","CI","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","ET","FK","FO","FJ","FI","FR","GF","PF","TF","GA","GM","GE","DE","GH","GI","GR","GL","GD","GP","GU","GT","GG","GN","GW","GY","HT","HM","VA","HN","HK","HU","IS","IN","ID","IR","IQ","IE","IM","IL","IT","JM","JP","JE","JO","KZ","KE","KI","KP","KR","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MO","MK","MG","MW","MY","MV","ML","MT","MH","MQ","MR","MU","YT","MX","FM","MD","MC","MN","MS","MA","MZ","MM","NA","NR","NP","NL","AN","NC","NZ","NI","NE","NG","NU","NF","MP","NO","OM","PK","PW","PS","PA","PG","PY","PE","PH","PN","PL","PT","PR","QA","RE","RO","RU","RW","SH","KN","LC","PM","VC","WS","SM","ST","SA","SN","CS","SC","SL","SG","SK","SI","SB","SO","ZA","GS","ES","LK","SD","SR","SJ","SZ","SEw","CH","SY","TW","TJ","TZ","TH","TL","TG","TK","TO","TT","TN","TR","TM","TC","TV","UG","UA","AE","GB","US","UM","UY","UZ","VU","VE","VN","VI","VG","WF","EH","YE","ZM","ZW")
    val CodeCountries = Map("AF"->"Afghanistan",
      "AX"->"Åland Islands",
      "AL"->"Albania",
      "DZ"->"Algeria",
      "AS"->"American Samoa",
      "AD"->"AndorrA",
      "AO"->"Angola",
      "AI"->"Anguilla",
      "AQ"->"Antarctica",
      "AG"->"Antigua and Barbuda",
      "AR"->"Argentina",
      "AM"->"Armenia",
      "AW"->"Aruba",
      "AU"->"Australia",
      "AT"->"Austria",
      "AZ"->"Azerbaijan",
      "BS"->"Bahamas",
      "BH"->"Bahrain",
      "BD"->"Bangladesh",
      "BB"->"Barbados",
      "BY"->"Belarus",
      "BE"->"Belgium",
      "BZ"->"Belize",
      "BJ"->"Benin",
      "BM"->"Bermuda",
      "BT"->"Bhutan",
      "BO"->"Bolivia",
      "BA"->"Bosnia and Herzegovina",
      "BW"->"Botswana",
      "BV"->"Bouvet Island",
      "BR"->"Brazil",
      "IO"->"British Indian Ocean Territory",
      "BN"->"Brunei Darussalam",
      "BG"->"Bulgaria",
      "BF"->"Burkina Faso",
      "BI"->"Burundi",
      "KH"->"Cambodia",
      "CM"->"Cameroon",
      "CA"->"Canada",
      "CV"->"Cape Verde",
      "KY"->"Cayman Islands",
      "CF"->"Central African Republic",
      "TD"->"Chad",
      "CL"->"Chile",
      "CN"->"China",
      "CX"->"Christmas Island",
      "CC"->"Cocos (Keeling) Islands",
      "CO"->"Colombia",
      "KM"->"Comoros",
      "CG"->"Congo",
      "CD"->"Congo , The Democratic",
      "CK"->"Cook Islands",
      "CR"->"Costa Rica",
      "CI"->"Cote DIvoire",
      "HR"->"Croatia",
      "CU"->"Cuba",
      "CY"->"Cyprus",
      "CZ"->"Czech Republic",
      "DK"->"Denmark",
      "DJ"->"Djibouti",
      "DM"->"Dominica",
      "DO"->"Dominican Republic",
      "EC"->"Ecuador",
      "EG"->"Egypt",
      "SV"->"El Salvador",
      "GQ"->"Equatorial Guinea",
      "ER"->"Eritrea",
      "EE"->"Estonia",
      "ET"->"Ethiopia",
      "FK"->"Falkland Islands (Malvinas)",
      "FO"->"Faroe Islands",
      "FJ"->"Fiji",
      "FI"->"Finland",
      "FR"->"France",
      "GF"->"French Guiana",
      "PF"->"French Polynesia",
      "TF"->"French Southern Territories",
      "GA"->"Gabon",
      "GM"->"Gambia",
      "GE"->"Georgia",
      "DE"->"Germany",
      "GH"->"Ghana",
      "GI"->"Gibraltar",
      "GR"->"Greece",
      "GL"->"Greenland",
      "GD"->"Grenada",
      "GP"->"Guadeloupe",
      "GU"->"Guam",
      "GT"->"Guatemala",
      "GG"->"Guernsey",
      "GN"->"Guinea",
      "GW"->"Guinea-Bissau",
      "GY"->"Guyana",
      "HT"->"Haiti",
      "HM"->"Heard Island and Mcdonald Islands",
      "VA"->"Holy See (Vatican City State)",
      "HN"->"Honduras",
      "HK"->"Hong Kong",
      "HU"->"Hungary",
      "IS"->"Iceland",
      "IN"->"India",
      "ID"->"Indonesia",
      "IR"->"Iran",
      "IQ"->"Iraq",
      "IE"->"Ireland",
      "IM"->"Isle of Man",
      "IL"->"Israel",
      "IT"->"Italy",
      "JM"->"Jamaica",
      "JP"->"Japan",
      "JE"->"Jersey",
      "JO"->"Jordan",
      "KZ"->"Kazakhstan",
      "KE"->"Kenya",
      "KI"->"Kiribati",
      "KP"->"Korea Democratic People",
      "KR"->"Korea Republic",
      "KW"->"Kuwait",
      "KG"->"Kyrgyzstan",
      "LA"->"Lao PeopleS Democratic Republic",
      "LV"->"Latvia",
      "LB"->"Lebanon",
      "LS"->"Lesotho",
      "LR"->"Liberia",
      "LY"->"Libyan Arab Jamahiriya",
      "LI"->"Liechtenstein",
      "LT"->"Lithuania",
      "LU"->"Luxembourg",
      "MO"->"Macao",
      "MK"->"Macedonia",
      "MG"->"Madagascar",
      "MW"->"Malawi",
      "MY"->"Malaysia",
      "MV"->"Maldives",
      "ML"->"Mali",
      "MT"->"Malta",
      "MH"->"Marshall Islands",
      "MQ"->"Martinique",
      "MR"->"Mauritania",
      "MU"->"Mauritius",
      "YT"->"Mayotte",
      "MX"->"Mexico",
      "FM"->"Micronesia",
      "MD"->"Moldova",
      "MC"->"Monaco",
      "MN"->"Mongolia",
      "MS"->"Montserrat",
      "MA"->"Morocco",
      "MZ"->"Mozambique",
      "MM"->"Myanmar",
      "NA"->"Namibia",
      "NR"->"Nauru",
      "NP"->"Nepal",
      "NL"->"Netherlands",
      "AN"->"Netherlands Antilles",
      "NC"->"New Caledonia",
      "NZ"->"New Zealand",
      "NI"->"Nicaragua",
      "NE"->"Niger",
      "NG"->"Nigeria",
      "NU"->"Niue",
      "NF"->"Norfolk Island",
      "MP"->"Northern Mariana Islands",
      "NO"->"Norway",
      "OM"->"Oman",
      "PK"->"Pakistan",
      "PW"->"Palau",
      "PS"->"Palestinian Territory",
      "PA"->"Panama",
      "PG"->"Papua New Guinea",
      "PY"->"Paraguay",
      "PE"->"Peru",
      "PH"->"Philippines",
      "PN"->"Pitcairn",
      "PL"->"Poland",
      "PT"->"Portugal",
      "PR"->"Puerto Rico",
      "QA"->"Qatar",
      "RE"->"Reunion",
      "RO"->"Romania",
      "RU"->"Russian Federation",
      "RW"->"RWANDA",
      "SH"->"Saint Helena",
      "KN"->"Saint Kitts and Nevis",
      "LC"->"Saint Lucia",
      "PM"->"Saint Pierre and Miquelon",
      "VC"->"Saint Vincent and the Grenadines",
      "WS"->"Samoa",
      "SM"->"San Marino",
      "ST"->"Sao Tome and Principe",
      "SA"->"Saudi Arabia",
      "SN"->"Senegal",
      "CS"->"Serbia and Montenegro",
      "SC"->"Seychelles",
      "SL"->"Sierra Leone",
      "SG"->"Singapore",
      "SK"->"Slovakia",
      "SI"->"Slovenia",
      "SB"->"Solomon Islands",
      "SO"->"Somalia",
      "ZA"->"South Africa",
      "GS"->"South Georgia and the South Sandwich Islands",
      "ES"->"Spain",
      "LK"->"Sri Lanka",
      "SD"->"Sudan",
      "SR"->"Suriname",
      "SJ"->"Svalbard and Jan Mayen",
      "SZ"->"Swaziland",
      "SE"->"Sweden",
      "CH"->"Switzerland",
      "SY"->"Syrian Arab Republic",
      "TW"->"Taiwan",
      "TJ"->"Tajikistan",
      "TZ"->"Tanzania",
      "TH"->"Thailand",
      "TL"->"Timor-Leste",
      "TG"->"Togo",
      "TK"->"Tokelau",
      "TO"->"Tonga",
      "TT"->"Trinidad and Tobago",
      "TN"->"Tunisia",
      "TR"->"Turkey",
      "TM"->"Turkmenistan",
      "TC"->"Turks and Caicos Islands",
      "TV"->"Tuvalu",
      "UG"->"Uganda",
      "UA"->"Ukraine",
      "AE"->"United Arab Emirates",
      "GB"->"United Kingdom",
      "US"->"United States",
      "UM"->"United States Minor Outlying Islands",
      "UY"->"Uruguay",
      "UZ"->"Uzbekistan",
      "VU"->"Vanuatu",
      "VE"->"Venezuela",
      "VN"->"Viet Nam",
      "VG"->"Virgin Islands,British",
      "VI"->"Virgin Islands,U.S",
      "WF"->"Wallis and Futuna",
      "EH"->"Western Sahara",
      "YE"->"Yemen",
      "ZM"->"Zambia",
      "ZW"->"Zimbabwe"
    )

    var countryOrCode = fields(fields.length-1).trim

    if(countriesSet.contains(countryOrCode)) return countryOrCode
    else if(countriesCodeSet.contains(countryOrCode))
    {   return CodeCountries(countryOrCode) }
    else return null
  }






}
