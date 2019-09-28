package com.weibo.dip.iplibrary.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Generate ip library.
 *
 * @author yurun
 */
public class GenerateIpLibraryMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateIpLibraryMain.class);

  public static class District {
    private String id;
    private String cn;
    private String en;

    /**
     * Construct a District instance.
     *
     * @param id ID
     * @param cn chinese name
     * @param en english name
     */
    public District(String id, String cn, String en) {
      this.id = id;
      this.cn = cn;
      this.en = en;
    }

    public String getId() {
      return id;
    }

    public String getCn() {
      return cn != null ? cn : getEn();
    }

    public String getEn() {
      return en != null ? en : StringUtils.EMPTY;
    }

    @Override
    public String toString() {
      return "District{" + "id='" + id + '\'' + ", cn='" + cn + '\'' + ", en='" + en + '\'' + '}';
    }
  }

  public static class City {
    private String id;
    private String cn;
    private String en;

    // ID to District
    private Map<String, District> districts = new HashMap<>();

    /**
     * Construct a City instance.
     *
     * @param id ID
     * @param cn chinese name
     * @param en english name
     */
    public City(String id, String cn, String en) {
      this.id = id;
      this.cn = cn;
      this.en = en;
    }

    public String getId() {
      return id;
    }

    public String getCn() {
      return cn != null ? cn : getEn();
    }

    public String getEn() {
      return en != null ? en : StringUtils.EMPTY;
    }

    public void putDistrict(String id, District district) {
      districts.put(id, district);
    }

    public District getDistrictById(String id) {
      return districts.get(id);
    }

    @Override
    public String toString() {
      return "City{"
          + "id='"
          + id
          + '\''
          + ", cn='"
          + cn
          + '\''
          + ", en='"
          + en
          + '\''
          + ", districts="
          + districts
          + '}';
    }
  }

  public static class Province {
    private String id;
    private String cn;
    private String en;

    // ID to City
    private Map<String, City> cities = new HashMap<>();

    /**
     * Construct a Province instalce.
     *
     * @param id ID
     * @param cn chinese name
     * @param en english name
     */
    public Province(String id, String cn, String en) {
      this.id = id;
      this.cn = cn;
      this.en = en;
    }

    public String getId() {
      return id;
    }

    public String getCn() {
      return cn != null ? cn : getEn();
    }

    public String getEn() {
      return en != null ? en : StringUtils.EMPTY;
    }

    public void putCity(String id, City city) {
      cities.put(id, city);
    }

    public City getCityById(String id) {
      return cities.get(id);
    }

    @Override
    public String toString() {
      return "Province{"
          + "id='"
          + id
          + '\''
          + ", cn='"
          + cn
          + '\''
          + ", en='"
          + en
          + '\''
          + ", cities="
          + cities
          + '}';
    }
  }

  public static class Country {
    private String id;
    private String code;
    private String fips;
    private String cn;
    private String en;

    // ID to Province
    private Map<String, Province> provinces = new HashMap<>();

    /**
     * Construct a Country instance.
     *
     * @param id ID
     * @param code country code
     * @param fips country fips
     * @param cn chinese name
     * @param en english name
     */
    public Country(String id, String code, String fips, String cn, String en) {
      this.id = id;
      this.code = code;
      this.fips = fips;
      this.cn = cn;
      this.en = en;
    }

    public String getId() {
      return id;
    }

    public String getCode() {
      return code;
    }

    public String getFips() {
      return fips;
    }

    public String getCn() {
      return cn != null ? cn : getEn();
    }

    public String getEn() {
      return en != null ? en : StringUtils.EMPTY;
    }

    public void putProvince(String id, Province region) {
      provinces.put(id, region);
    }

    public Province getProvinceById(String id) {
      return provinces.get(id);
    }

    @Override
    public String toString() {
      return "Country{"
          + "id='"
          + id
          + '\''
          + ", code='"
          + code
          + '\''
          + ", fips='"
          + fips
          + '\''
          + ", cn='"
          + cn
          + '\''
          + ", en='"
          + en
          + '\''
          + ", provinces="
          + provinces
          + '}';
    }
  }

  public static class Isp {
    private String id;
    private String name;

    /**
     * Construct a Isp instace.
     *
     * @param id ID
     * @param name name
     */
    public Isp(String id, String name) {
      this.id = id;
      this.name = name;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name != null ? name : StringUtils.EMPTY;
    }

    @Override
    public String toString() {
      return "Isp{" + "id='" + id + '\'' + ", name='" + name + '\'' + '}';
    }
  }

  public static class Type {
    private String id;
    private String name;

    /**
     * Construct a Type instance.
     *
     * @param id ID
     * @param name name
     */
    public Type(String id, String name) {
      this.id = id;
      this.name = name;
    }

    public String getId() {
      return id;
    }

    public String getName() {
      return name != null ? name : StringUtils.EMPTY;
    }

    @Override
    public String toString() {
      return "Type{" + "id='" + id + '\'' + ", name='" + name + '\'' + '}';
    }
  }

  public static class IpData {
    private long startIpInLong;
    private long endIpInLong;
    private String startIpInStr;
    private String endIpInStr;
    private String country;
    private String province;
    private String city;
    private String district;
    private String isp;
    private String type;
    private String desc;

    /**
     * Construct a IpData instalce.
     *
     * @param startIpInLong start ip, represendted as an long(include)
     * @param endIpInLong end ip, represented as an long(include)
     * @param startIpInStr start ip address
     * @param endIpInStr end ip address
     * @param country country id
     * @param province province id
     * @param city city id
     * @param district district id
     * @param isp isp id
     * @param type type id
     * @param desc desc info
     */
    public IpData(
        long startIpInLong,
        long endIpInLong,
        String startIpInStr,
        String endIpInStr,
        String country,
        String province,
        String city,
        String district,
        String isp,
        String type,
        String desc) {
      this.startIpInLong = startIpInLong;
      this.endIpInLong = endIpInLong;
      this.startIpInStr = startIpInStr;
      this.endIpInStr = endIpInStr;
      this.country = country;
      this.province = province;
      this.city = city;
      this.district = district;
      this.isp = isp;
      this.type = type;
      this.desc = desc;
    }

    public long getStartIpInLong() {
      return startIpInLong;
    }

    public long getEndIpInLong() {
      return endIpInLong;
    }

    public String getStartIpInStr() {
      return startIpInStr;
    }

    public String getEndIpInStr() {
      return endIpInStr;
    }

    public String getCountry() {
      return country;
    }

    public String getProvince() {
      return province;
    }

    public String getCity() {
      return city;
    }

    public String getDistrict() {
      return district;
    }

    public String getIsp() {
      return isp;
    }

    public String getType() {
      return type;
    }

    public String getDesc() {
      return desc;
    }

    @Override
    public String toString() {
      return "IpData{"
          + "startIpInLong="
          + startIpInLong
          + ", endIpInLong="
          + endIpInLong
          + ", startIpInStr='"
          + startIpInStr
          + '\''
          + ", endIpInStr='"
          + endIpInStr
          + '\''
          + ", country='"
          + country
          + '\''
          + ", province='"
          + province
          + '\''
          + ", city='"
          + city
          + '\''
          + ", district='"
          + district
          + '\''
          + ", isp='"
          + isp
          + '\''
          + ", type='"
          + type
          + '\''
          + ", desc='"
          + desc
          + '\''
          + '}';
    }
  }

  private static final Map<String, Country> COUNTRIES = new HashMap<>();
  private static final Map<String, Isp> ISPS = new HashMap<>();
  private static final Map<String, Type> TYPES = new HashMap<>();
  private static final SortedMap<Long, IpData> IPDATAS = new TreeMap<>();

  private static final Long[] IPPOINTS;

  static {
    try {
      initializeCountries();
      initializeIsps();
      initializeTypes();
      initializeIpDatas();

      IPPOINTS = IPDATAS.keySet().toArray(new Long[0]);
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static void initializeCountries() throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    DocumentBuilder db = factory.newDocumentBuilder();

    InputStream xmlIn = GenerateIpLibraryMain.class.getResourceAsStream("/region.xml");

    Document xmlDoc = db.parse(xmlIn);

    NodeList countryNodes = xmlDoc.getElementsByTagName("countries");

    if (countryNodes == null || countryNodes.getLength() <= 0) {
      return;
    }

    for (int countryIndex = 0; countryIndex < countryNodes.getLength(); countryIndex++) {
      Element countryElem = (Element) countryNodes.item(countryIndex);

      String countryId = null;
      String countryCode = null;
      String countryFips = null;
      String countryCn = null;
      String countryEn = null;

      NodeList countryIds = countryElem.getElementsByTagName("id");
      if (countryIds != null && countryIds.getLength() > 0) {
        countryId = countryIds.item(0).getTextContent();
      }

      if (countryId == null || countryId.isEmpty()) {
        continue;
      }

      NodeList countryCodes = countryElem.getElementsByTagName("code");
      if (countryCodes != null && countryCodes.getLength() > 0) {
        countryCode = countryCodes.item(0).getTextContent();
      }

      NodeList countryFipses = countryElem.getElementsByTagName("fips");
      if (countryFipses != null && countryFipses.getLength() > 0) {
        countryFips = countryFipses.item(0).getTextContent();
      }

      NodeList countryCns = countryElem.getElementsByTagName("cn");
      if (countryCns != null && countryCns.getLength() > 0) {
        countryCn = countryCns.item(0).getTextContent();
      }

      NodeList countryEns = countryElem.getElementsByTagName("en");
      if (countryEns != null && countryEns.getLength() > 0) {
        countryEn = countryEns.item(0).getTextContent();
      }

      // 国家
      Country country = new Country(countryId, countryCode, countryFips, countryCn, countryEn);

      COUNTRIES.put(countryId, country);

      NodeList regions = countryElem.getElementsByTagName("regions");

      if (regions == null || regions.getLength() <= 0) {
        continue;
      }

      for (int regionIndex = 0; regionIndex < regions.getLength(); regionIndex++) {
        Element regionElem = (Element) regions.item(regionIndex);

        String regionId = null;
        String regionCn = null;
        String regionEn = null;

        NodeList regionIds = regionElem.getElementsByTagName("id");
        if (regionIds != null && regionIds.getLength() > 0) {
          regionId = regionIds.item(0).getTextContent();
        }

        if (regionId == null || regionId.isEmpty()) {
          continue;
        }

        NodeList regionCns = regionElem.getElementsByTagName("cn");
        if (regionCns != null && regionCns.getLength() > 0) {
          regionCn = regionCns.item(0).getTextContent();
        }

        NodeList regionEns = regionElem.getElementsByTagName("en");
        if (regionEns != null && regionEns.getLength() > 0) {
          regionEn = regionEns.item(0).getTextContent();
        }

        // 省份
        Province region = new Province(regionId, regionCn, regionEn);

        country.putProvince(regionId, region);

        NodeList cities = regionElem.getElementsByTagName("cities");
        if (cities == null || cities.getLength() <= 0) {
          continue;
        }

        for (int cityIndex = 0; cityIndex < cities.getLength(); cityIndex++) {
          Element cityElem = (Element) cities.item(cityIndex);

          String cityId = null;
          String cityCn = null;
          String cityEn = null;

          NodeList cityIds = cityElem.getElementsByTagName("id");
          if (cityIds != null && cityIds.getLength() > 0) {
            cityId = cityIds.item(0).getTextContent();
          }

          if (cityId == null || cityId.isEmpty()) {
            continue;
          }

          NodeList cityCns = cityElem.getElementsByTagName("cn");
          if (cityCns != null && cityCns.getLength() > 0) {
            cityCn = cityCns.item(0).getTextContent();
          }

          NodeList cityEns = cityElem.getElementsByTagName("en");
          if (cityEns != null && cityEns.getLength() > 0) {
            cityEn = cityEns.item(0).getTextContent();
          }

          // 城市
          City city = new City(cityId, cityCn, cityEn);

          region.putCity(cityId, city);

          NodeList districts = cityElem.getElementsByTagName("districts");

          if (districts == null || districts.getLength() <= 0) {
            continue;
          }

          for (int districtIndex = 0; districtIndex < districts.getLength(); districtIndex++) {
            Element districtElem = (Element) districts.item(districtIndex);

            String districtId = null;
            String districtCn = null;
            String districtEn = null;

            NodeList districtIds = districtElem.getElementsByTagName("id");
            if (districtIds != null && districtIds.getLength() > 0) {
              districtId = districtIds.item(0).getTextContent();
            }

            NodeList districtCns = districtElem.getElementsByTagName("cn");
            if (districtCns != null && districtCns.getLength() > 0) {
              districtCn = districtCns.item(0).getTextContent();
            }

            NodeList districtEns = districtElem.getElementsByTagName("en");
            if (districtEns != null && districtEns.getLength() > 0) {
              districtEn = districtEns.item(0).getTextContent();
            }

            // 区域
            District district = new District(districtId, districtCn, districtEn);

            city.putDistrict(districtId, district);
          }
        }
      }
    }

    LOGGER.info("initialize countries success");
  }

  private static void initializeIsps() throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    DocumentBuilder db = factory.newDocumentBuilder();

    InputStream xmlIn = GenerateIpLibraryMain.class.getResourceAsStream("/isp.xml");

    Document xmlDoc = db.parse(xmlIn);

    NodeList entryNodes = xmlDoc.getElementsByTagName("entry");
    if (entryNodes == null || entryNodes.getLength() <= 0) {
      return;
    }

    for (int entryIndex = 0; entryIndex < entryNodes.getLength(); entryIndex++) {
      Element ispElem = (Element) entryNodes.item(entryIndex);

      String ispId = null;

      String ispName = null;

      NodeList ispIds = ispElem.getElementsByTagName("id");
      if (ispIds != null && ispIds.getLength() > 0) {
        ispId = ispIds.item(0).getTextContent();
      }

      if (ispId == null || ispId.isEmpty()) {
        continue;
      }

      NodeList ispNames = ispElem.getElementsByTagName("name");
      if (ispNames != null && ispNames.getLength() > 0) {
        ispName = ispNames.item(0).getTextContent();
      }

      Isp isp = new Isp(ispId, ispName);

      ISPS.put(ispId, isp);
    }

    LOGGER.info("initialize isps success");
  }

  private static void initializeTypes() throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    DocumentBuilder db = factory.newDocumentBuilder();

    InputStream xmlIn = GenerateIpLibraryMain.class.getResourceAsStream("/type.xml");

    Document xmlDoc = db.parse(xmlIn);

    NodeList entryNodes = xmlDoc.getElementsByTagName("entry");
    if (entryNodes == null || entryNodes.getLength() <= 0) {
      return;
    }

    for (int entryIndex = 0; entryIndex < entryNodes.getLength(); entryIndex++) {
      Element typeElem = (Element) entryNodes.item(entryIndex);

      String typeId = null;

      String typeName = null;

      NodeList ispIds = typeElem.getElementsByTagName("id");
      if (ispIds != null && ispIds.getLength() > 0) {
        typeId = ispIds.item(0).getTextContent();
      }

      if (typeId == null || typeId.isEmpty()) {
        continue;
      }

      NodeList ispNames = typeElem.getElementsByTagName("name");
      if (ispNames != null && ispNames.getLength() > 0) {
        typeName = ispNames.item(0).getTextContent();
      }

      Type type = new Type(typeId, typeName);

      TYPES.put(typeId, type);
    }

    LOGGER.info("initialize types success");
  }

  private static void initializeIpDatas() throws Exception {
    InputStream in = GenerateIpLibraryMain.class.getResourceAsStream("/ipdata.csv");

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

      String line;

      while ((line = reader.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }

        String[] words = line.split("\t", -1);

        if (words.length != 11) {
          continue;
        }

        try {
          long startIpInLong = Long.parseLong(words[0]);
          long endIpInLong = Long.parseLong(words[1]);
          String startIpInStr = words[2];
          String endIpInStr = words[3];
          String country = words[4];
          String province = words[5];
          String city = words[6];
          String district = words[7];
          String isp = words[8];
          String type = words[9];
          String desc = words[10];

          IpData ipData =
              new IpData(
                  startIpInLong,
                  endIpInLong,
                  startIpInStr,
                  endIpInStr,
                  country,
                  province,
                  city,
                  district,
                  isp,
                  type,
                  desc);

          IPDATAS.put(endIpInLong, ipData);
        } catch (Exception e) {
          LOGGER.warn("ipdata wrong line: " + line);
        }
      }
    }

    LOGGER.info("initialize ipDatas success");
  }

  public static Map<String, Country> getCountries() {
    return COUNTRIES;
  }

  public static Map<String, Isp> getIsps() {
    return ISPS;
  }

  public static Map<String, Type> getTypes() {
    return TYPES;
  }

  public static SortedMap<Long, IpData> getIpdatas() {
    return IPDATAS;
  }

  /**
   * Create ip library index files.
   *
   * @param indexDir a directory for storing files
   * @throws Exception if create error
   */
  public static void create(String indexDir) throws Exception {
    final Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    Preconditions.checkState(StringUtils.isNotEmpty(indexDir), "indexDir is empty");

    File directory = new File(indexDir);

    LOGGER.info("index dir: " + directory);

    Preconditions.checkState(!directory.exists(), "ip library %s already exist", indexDir);

    directory.mkdirs();

    File ippoints = new File(directory, "ippoints");

    ippoints.createNewFile();

    Directory dir = FSDirectory.open(new File(directory, "index"));

    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);

    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);

    iwc.setOpenMode(OpenMode.CREATE);

    try (BufferedWriter ipPointsWriter =
            new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(ippoints), CharEncoding.UTF_8));
        IndexWriter indexWriter = new IndexWriter(dir, iwc)) {

      Map<String, Country> countries = GenerateIpLibraryMain.getCountries();
      Map<String, Isp> isps = GenerateIpLibraryMain.getIsps();
      Map<String, Type> types = GenerateIpLibraryMain.getTypes();
      SortedMap<Long, IpData> ipdatas = GenerateIpLibraryMain.getIpdatas();

      ipPointsWriter.write(String.valueOf(ipdatas.size()));
      ipPointsWriter.newLine();

      for (Entry<Long, IpData> entry : ipdatas.entrySet()) {
        IpData ipData = entry.getValue();

        ipPointsWriter.write(String.valueOf(ipData.getEndIpInLong()));
        ipPointsWriter.newLine();

        Country country = countries.get(ipData.getCountry());

        Province province = null;
        if (country != null) {
          province = country.getProvinceById(ipData.getProvince());
        }

        City city = null;
        if (province != null) {
          city = province.getCityById(ipData.getCity());
        }

        District district = null;
        if (city != null) {
          district = city.getDistrictById(ipData.getDistrict());
        }

        Isp isp = isps.get(ipData.getIsp());

        Type type = types.get(ipData.getType());

        String startIpCol = String.valueOf(ipData.getStartIpInLong());
        String endIpCol = String.valueOf(ipData.getEndIpInLong());
        String countryCol = Objects.nonNull(country) ? country.getCn() : StringUtils.EMPTY;
        String provinceCol = Objects.nonNull(province) ? province.getCn() : StringUtils.EMPTY;
        String cityCol = Objects.nonNull(city) ? city.getCn() : StringUtils.EMPTY;
        String districtCol = Objects.nonNull(district) ? district.getCn() : StringUtils.EMPTY;
        String ispCol = Objects.nonNull(isp) ? isp.getName() : StringUtils.EMPTY;
        String typeCol = Objects.nonNull(type) ? type.getName() : StringUtils.EMPTY;
        String descCol = Objects.nonNull(ipData.getDesc()) ? ipData.getDesc() : StringUtils.EMPTY;

        org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();

        document.add(new StringField("0", startIpCol, Store.YES));
        document.add(new StringField("1", endIpCol, Store.YES));
        document.add(new StringField("2", countryCol.trim(), Store.YES));
        document.add(new StringField("3", provinceCol.trim(), Store.YES));
        document.add(new StringField("4", cityCol.trim(), Store.YES));
        document.add(new StringField("5", districtCol.trim(), Store.YES));
        document.add(new StringField("6", ispCol.trim(), Store.YES));
        document.add(new StringField("7", typeCol.trim(), Store.YES));
        document.add(new StringField("8", descCol.trim(), Store.YES));

        indexWriter.addDocument(document);
      }
    }

    stopwatch.stop();

    LOGGER.info(
        "create ip library {} success, consume time: {} ms",
        indexDir,
        stopwatch.elapsedTime(TimeUnit.MILLISECONDS));
  }

  /**
   * Main.
   *
   * @param args args[0] a directory for storing ip library index files
   */
  public static void main(String[] args) {
    try {
      create(args[0]);

      System.exit(0);
    } catch (Exception e) {
      LOGGER.error("generate ip library error: {}", ExceptionUtils.getFullStackTrace(e));

      System.exit(-1);
    }
  }
}
