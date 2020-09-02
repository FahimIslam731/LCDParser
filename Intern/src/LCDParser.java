import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.InputStream;
import java.sql.PreparedStatement;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class LCDParser {
	private final String DELIMITER = ", ";
	static DataWriter dw;
	String username;
	String password;
	InputStream is;

	public LCDParser() throws Exception {
		Properties prop = getPropValues();

		dw = new DataWriter(prop.getProperty("classpath"), prop.getProperty("url"), prop.getProperty("username"),
				prop.getProperty("password"), prop.getProperty("db"));
	}

	public Properties getPropValues() throws Exception {
		Properties prop = new Properties();
		try {
			String fileName = "config.properties";

			is = getClass().getClassLoader().getResourceAsStream(fileName);

			if (is != null) {
				prop.load(is);
			} else {
				throw new FileNotFoundException("property file '" + fileName + "' not found in the classpath");
			}
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			is.close();
		}
		return prop;
	}

	public static void main(String args[]) throws Exception {
		long start = System.currentTimeMillis();
		LCDParser lcd = new LCDParser();

		ArrayList<String> files = getFiles();

		System.out.println("Processing CPT to ICD");
		for (int i = 0; i < files.size(); i++) {
			lcd.run(files.get(i));
		}
		System.out.println("Processing BillCodes");
		lcd.runBillCodes(files);
		System.out.println("Processing Contractors");
		lcd.runContractors(files);
		long totalTime = System.currentTimeMillis() - start;
		System.out.println("time(sec): " + (totalTime / 1000));
	}

	private void runContractors(ArrayList<String> files) throws Exception {
		Map<String, String> map = new HashMap<>();
		for (int i = 0; i < files.size(); i++) {
			JSONObject jsonFile = (JSONObject) (new JSONParser()
					.parse(new FileReader("json//actualJsons//" + files.get(i))));
			JSONArray contractors = (JSONArray) jsonFile.get("contractors");
			for (int j = 0; j < contractors.size(); j++) {
				Map m = ((Map) (contractors.get(j)));
				map.put("contractor_bus_name", (String) m.get("contractor_bus_name"));
				map.put("description", (String) m.get("description"));
				map.put("contractor_number", (String) m.get("contractor_number"));
				map.put("sub_type_description", (String) m.get("subtype_description"));
				map.put("super_mac_description", (String) m.get("super_mac_description"));
				map.put("states", (String) m.get("states"));
				dw.insertInto(map, "contractors");
			}
			dw.execute();
			map.clear();
		}
	}

	private void runBillCodes(ArrayList<String> infoFile) throws Exception {
		PrintWriter pw = new PrintWriter("outputs/lcd_billCodes.csv");
		pw.println("lcd_id, bill_code_id, description");
		for (int i = 0; i < infoFile.size(); i++) {
			printOut(infoFile.get(i), pw);
		}
		pw.close();
	}

	private void printOut(String file, PrintWriter pw) throws Exception, ParseException, FileNotFoundException {
		Map<String, String> map1 = new HashMap<>();
		JSONObject jsonFile = (JSONObject) (new JSONParser().parse(new FileReader("json//actualJsons//" + file)));
		JSONArray billCodes = (JSONArray) jsonFile.get("billCodes");
		for (int i = 0; i < billCodes.size(); i++) {
			Map map = (Map) (billCodes.get(i));
			String lcd_id = (Long) (map.get("lcd_id")) + "";
			String bill_code_id = (Long) map.get("bill_code_id") + "";
			String description = (String) map.get("description") + "";
			pw.println(lcd_id + ", " + bill_code_id + ", " + description);
			map1.put("description", description);
			map1.put("bill_code_id", bill_code_id);
			map1.put("lcd_id", lcd_id);
			dw.insertInto(map1, "billing_code");
		}
		dw.execute();

	}

	private static ArrayList<String> getFiles() throws Exception {
		Scanner fileReader = new Scanner(new File("json/jsons.txt"));
		ArrayList<String> files = new ArrayList<>();
		while (fileReader.hasNext()) {
			files.add(fileReader.nextLine());
		}
		return files;
	}

	private void run(String infoFile) throws Exception {
		JSONObject jsonFile = (JSONObject) (new JSONParser().parse(new FileReader("json//actualJsons//" + infoFile)));
		PrintWriter pw = new PrintWriter("outputs/" + infoFile.substring(0, 6) + ".csv");
		JSONArray ja = (JSONArray) jsonFile.get("icdCodes");

		pw.println("CPT_CODES, CPT_DESCRIPTION, ICD_CODES, ICD_DESCRIPTION");
		Map hcpc = makeHashMapofHCPCCodes(jsonFile);

		for (int i = 0; i < ja.size(); i++) {
			Map map = (Map) (ja.get(i));
			String paragraph = (String) map.get("paragraph");
			JSONArray children = (JSONArray) map.get("children");
			ArrayList<String> cptList = getCodes(paragraph);
			if (cptList.isEmpty()) {
				cptList = new ArrayList<>(hcpc.keySet());
			}
			putCPTCodes(pw, cptList, getIds(children), hcpc, getICDDescriptions(children), infoFile);
		}
		dw.execute();

		pw.close();
	}

	private ArrayList<String> getICDDescriptions(JSONArray c) {
		List<String> descriptions = new ArrayList<>();
		for (int i = 0; i < c.size(); i++) {
			Map m = (Map) c.get(i);
			descriptions.add(m.get("description").toString());
		}
		return (ArrayList<String>) descriptions;
	}

	private Map<String, String> makeHashMapofHCPCCodes(JSONObject jsonFile) {
		Map<String, String> map1 = new HashMap();
		JSONArray hcpcCodes = (JSONArray) jsonFile.get("hcpcCodes");
		for (int i = 0; i < hcpcCodes.size(); i++) {
			Map map2 = (Map) (hcpcCodes.get(i));
			JSONArray children = (JSONArray) map2.get("children");
			for (int j = 0; j < children.size(); j++) {
				Map mapChild = (Map) children.get(j);
				map1.put((String) (mapChild.get("hcpc_code_id")), (String) (mapChild.get("short_description"))
						+ "<<<wut>>>" + (String) (mapChild.get("long_description")));

			}
		}
		return map1;
	}

	private List<String> getIds(JSONArray c) {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < c.size(); i++) {
			Map m = (Map) c.get(i);
			ids.add(m.get("icd10_code_id").toString());
		}
		return ids;
	}

	private ArrayList<String> getCodes(String paragraph) {
		ArrayList<String> arr = new ArrayList<>();
		if (paragraph == null) {
			return arr;
		}
		Pattern p = Pattern.compile("\\d{5}");
		Matcher m = p.matcher(paragraph);
		while (m.find()) {
			arr.add(m.group());
		}
		ArrayList<String> al = lookForDashes(paragraph);
		for (int i = 0; i < al.size(); i++) {
			arr.add(al.get(i));
		}
		return arr;
	}

	private ArrayList<String> lookForDashes(String paragraph) {
		ArrayList<String> arr = new ArrayList<>();
		if (paragraph == null) {
			return arr;
		}
		Pattern p = Pattern.compile("\\d{5}-\\d{5}");
		Matcher m = p.matcher(paragraph);
		while (m.find()) {
			ArrayList<String> al = findValues(Integer.parseInt(m.group().substring(0, m.group().indexOf("-"))),
					Integer.parseInt(m.group().substring(m.group().indexOf("-") + 1)));

			for (int i = 0; i < al.size(); i++) {
				arr.add(al.get(i));
			}
		}
		return arr;
	}

	private ArrayList<String> findValues(int beg, int end) {
		ArrayList<String> al = new ArrayList<>();
		for (int i = beg + 1; i < end; i++) {
			al.add(i + "");
		}
		return al;
	}

	private void putCPTCodes(PrintWriter pw, List<String> arr, List<String> id, Map hcpc, ArrayList<String> desc,
			String file) throws Exception {
		Map<String, String> map1 = new HashMap<>();
		for (int i = 0; i < arr.size(); i++) {
			String s = arr.get(i);
			for (int j = 0; j < id.size(); j++) {
				if (hcpc.containsKey(s)) {
					pw.print(s + DELIMITER + hcpc.get(s) + DELIMITER + id.get(j) + DELIMITER + desc.get(j) + "\n");
					map1.put("cpt_code", s);
					map1.put("short_description",
							((String) hcpc.get(s)).substring(0, ((String) hcpc.get(s)).indexOf("<<<wut>>>")));
					map1.put("long_description",
							((String) hcpc.get(s)).substring(((String) hcpc.get(s)).indexOf("<<<wut>>>") + 9));
					map1.put("icd_code", id.get(j));
					map1.put("icd_description", desc.get(j));
					map1.put("lcd_id", file.substring(1, 6));
					dw.insertInto(map1, "cpt_to_icd");
				} else {
					pw.print("NOT PROPER CPT CODE.\n");
				}
			}
		}
	}
}
