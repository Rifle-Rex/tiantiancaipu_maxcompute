package drivers;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.google.gson.*;

import java.io.IOException;
import java.sql.*;
import java.util.Map.Entry;
import java.util.*;


public class FetchAttrFromRecipes {

    public static void main(String[] args) throws OdpsException {
        // args中，第一个参数是文章类型，第二个参数是pt分区

        if(args[0] == null || args[1] == null){
            return;
        }

        String articleType = args[0];
        String pt = args[1];
        String attrJson = "";
        try {

            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://192.168.1.169:3306/tiantiancaipu?characterEncoding=utf-8";
            String user = "root";
            String password = "123456";
            Connection conn = DriverManager.getConnection(url, user, password);

            // 获取文章的字段描述
            StringLengthComparator stringLengthComparator = new StringLengthComparator();
            PreparedStatement preStatement = conn.prepareStatement("select * from s_arc_type where typename = ?");
            Statement statement = conn.createStatement();
            preStatement.setString(1, articleType);
            ResultSet rs = preStatement.executeQuery();
            JsonParser parse = new JsonParser();  //创建json解析器
            JsonObject articleColumns;
            List<HashMap<String, Object>> attrDescript = new ArrayList<>();
            while (rs.next()) {
                String mainColumnsString = rs.getString("main_columns");
                String attrColumnsString = rs.getString("attr_columns");
                String[] allColumnsString = new String[]{mainColumnsString, attrColumnsString};
                for (String columnsString : allColumnsString) {
                    if (columnsString != null) {
                        articleColumns = (JsonObject) parse.parse(columnsString);
                        Set<Entry<String, JsonElement>> es = articleColumns.entrySet();
                        for (Entry<String, JsonElement> en : es) {
                            String columns = en.getKey();
                            String type = en.getValue().getAsJsonObject().get("type").getAsString();
                            if (type.equals("select") || type.equals("enum")) {
                                Boolean multiSelect = Boolean.FALSE;
                                if (en.getValue().getAsJsonObject().has("multi_select")){
                                    multiSelect = en.getValue().getAsJsonObject().get("multi_select").getAsBoolean();
                                }
                                HashMap<String, Object> rowHashMap = new HashMap<>();
                                rowHashMap.put("columns", columns);
                                rowHashMap.put("type", type);
                                rowHashMap.put("multi_select", multiSelect);
                                attrDescript.add(rowHashMap);
                            }
                        }
                    }
                }
            }
            rs.close();
            for (HashMap<String, Object> es: attrDescript){
                String key = (String) es.get("columns");
                String type = (String) es.get("type");
                ArrayList<String> values = new ArrayList<String>();
                if(type.equals("select")){
                    rs = statement.executeQuery(String.format("select * from s_attr where type = 0 and channel = 0 and name = '%s'", key));
                    Integer index_id = 0;
                    while (rs.next()){
                        values = new ArrayList<String>(Arrays.asList(rs.getString("value").split(",")));
                        index_id = rs.getInt("id");
                    }
                    if (index_id == 0){
                        es.put("values", new HashMap<String, ArrayList>());
                        continue;
                    }
                    rs.close();
                    // JsonElement synonyms = new JsonObject();
                    HashMap<String, ArrayList> synonyms = new HashMap<>();
                    rs = statement.executeQuery(String.format("select * from s_attr_synonyms where type = 'attr' and index_id = %d", index_id));
                    while (rs.next()){
                        String attrName = rs.getString("attr_name");
                        if (values.contains(attrName)) {
                            ArrayList<String> synonymsArrayList = new ArrayList<String>(Arrays.asList(rs.getString("synonyms").split(",")));
                            synonymsArrayList.add(attrName);
                            Collections.sort(synonymsArrayList, stringLengthComparator);
                            synonyms.put(attrName, synonymsArrayList);
                        }
                    }
                    rs.close();
                    for(String value: values){
                        if (!synonyms.containsKey(value)){
                            ArrayList<String> synonymsArrayList = new ArrayList<String>();
                            synonymsArrayList.add(value);
                            synonyms.put(value,synonymsArrayList);
                        }
                    }
                    es.put("values", synonyms);
                }
                else if(type.equals("enum")){
                    // 获取对用枚举的同义词记录
                    rs = statement.executeQuery(String.format("select * from s_attr_synonyms where type ='enum' and index_id = (select id from s_enum_index where egroup = '%s')", key));
                    HashMap<String, ArrayList> synonyms = new HashMap<>();
                    while(rs.next()){
                        String attrName = rs.getString("attr_name");
                        // Float attrValue = rs.getFloat("attr_value");
                        ArrayList<String> attrSynonyms = new ArrayList<String>(Arrays.asList(rs.getString("synonyms").split(",")));
                        attrSynonyms.add(attrName);
                        Collections.sort(attrSynonyms, new StringLengthComparator());
                        synonyms.put(attrName, attrSynonyms);
                    }
                    // 获取对应枚举值
                    ArrayList<HashMap<String, Object>> attrEnum = new ArrayList<>();
                    rs = statement.executeQuery(String.format("select * from s_enum where egroup = '%s'", key));
                    while (rs.next()){
                        HashMap<String, Object> rowEnum = new HashMap<>();
                        String ename = rs.getString("ename");
                        Float evalue = rs.getFloat("evalue");
                        rowEnum.put("ename", ename);
                        rowEnum.put("evalue", evalue);
                        if (synonyms.containsKey(ename)){
                            rowEnum.put("synonyms", synonyms.get(ename));
                        }
                        else{
                            rowEnum.put("synonyms", new ArrayList<String>(Arrays.asList(ename.split(","))));
                        }
                        attrEnum.add(rowEnum);
                    }
                    Collections.sort(attrEnum, new EvalueComparator());
                    es.put("values", attrEnum);
                }
                else{
                    continue;
                }
            }

            Gson gson = new Gson();
            attrJson = gson.toJson(attrDescript);
            // 关闭资源
            rs.close();
            preStatement.close();
            statement.close();
            conn.close();

        }
        catch (ClassNotFoundException e) {
            System.out.println(e);
            System.exit(1);
        }
        catch (SQLException e){
            System.out.println(e);
            System.exit(2);
        }


        System.out.println(attrJson);
        // System.exit(1);
        JobConf job = new JobConf();

        job.setStrings("attrJson", attrJson);
        // TODO: specify map output types
        // job.setMapOutputKeySchema(SchemaUtils.fromString( ?));
        // job.setMapOutputValueSchema(SchemaUtils.fromString( ?));

        //对于MapOnly的作业，必须显式设置reducer的个数为0。
        job.setNumReduceTasks(0);

        LinkedHashMap<String, String> ptMap = new LinkedHashMap<String, String>();
        ptMap.put("pt", pt);

        // TODO: specify input and output tables
        InputUtils.addTable(TableInfo.builder().tableName("dim_article_recipes").build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("dim_output_fetch_recipes_attr").build(), job);

        // TODO: specify a mapper
        job.setMapperClass(attrMapper.class);
        // TODO: specify a reducer
        // job.setReducerClass( ?);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();
    }

}

class attrMapper extends MapperBase{
    public Record tableOutput;
    public JsonArray attr;

    @Override
    public void setup(TaskContext context) throws IOException {
        this.tableOutput = context.createOutputRecord();
        String attrJson = (String)context.getJobConf().get("attrJson");
        if (!attrJson.isEmpty()) {
            JsonParser parse = new JsonParser();  //创建json解析器
            this.attr = (JsonArray) parse.parse(attrJson);
        }
        else {
            throw new IOException("not attrString.");
        }
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        HashMap<String, String> calResult = new HashMap<>();
        // 提取并解析step，提取内容并且拼接成一个string
        String content = record.getString("title");
        String title = record.getString("title");
        Long id = record.getBigint("id");
        String stepsString = record.getString("steps");
        Gson gson = new Gson();
        JsonParser parse = new JsonParser();
        JsonArray stepsJsonObject = (JsonArray) parse.parse(stepsString);
        Iterator stepsIterator = stepsJsonObject.iterator();
        while (stepsIterator.hasNext()){
            JsonObject oneStep = (JsonObject) stepsIterator.next();
            content = content.concat(oneStep.get("content").getAsString());
        }
        System.out.println(content);
        Iterator attrInterator = this.attr.iterator();
        // for(Entry<String,; JsonElement> oneAttr : this.attr.entrySet()){
        while (attrInterator.hasNext()){
            JsonObject jsonObject = (JsonObject) attrInterator.next();
            String columns = jsonObject.get("columns").getAsString();
            String type = jsonObject.get("type").getAsString();
            Boolean multiSelect = jsonObject.get("multi_select").getAsBoolean();
            if (type.equals("select")){
                JsonObject values = jsonObject.get("values").getAsJsonObject();
                // 当前字段是否多选
                if (multiSelect) {
                    ArrayList<String> attrCalValue = new ArrayList<>();
                    for (Entry<String, JsonElement> value : values.entrySet()) {
                        String attrString = record.getString(columns);
                        if (null == attrString || attrString.isEmpty()) {
                            attrString = "";
                        }
                        ArrayList<String> attrOriValue = new ArrayList<String>(Arrays.asList(attrString.split(",")));
                        String oneAttrValue = value.getKey();
                        if (attrOriValue.contains(oneAttrValue)) {
                            attrCalValue.add(oneAttrValue);
                            continue;
                        }
                        // 已经存在的属性保存
                        Iterator valuesInterator = value.getValue().getAsJsonArray().iterator();
                        while(valuesInterator.hasNext()) {
                            JsonElement oneValue = (JsonElement)valuesInterator.next();
                            String synonyms = oneValue.getAsString();
                            if (synonyms.length() > 1 && content.contains(synonyms)) {
                                attrCalValue.add(oneAttrValue);
                                break;
                            }
                            else if(synonyms.length() == 1 && title.contains(synonyms)){
                                attrCalValue.add(oneAttrValue);
                                break;
                            }
                        }
                    }
                    if (!attrCalValue.isEmpty()) {
                        calResult.put(columns, String.join(",", attrCalValue));
                    }
                }
                else {
                    String attrOriValue = record.getString(columns);
                    if (null != attrOriValue && !attrOriValue.isEmpty()){
                        continue;
                    }
                    for(Entry<String, JsonElement> value : values.entrySet()){
                        String attr = value.getKey();
                        JsonArray valueArray = value.getValue().getAsJsonArray();
                        Iterator valueIterator = valueArray.iterator();
                        while (valueIterator.hasNext()){
                            JsonElement synonyms = (JsonElement)valueIterator.next();
                            String synonyms_str = synonyms.getAsString();
                            if (synonyms_str.length() > 1 && content.contains(synonyms_str)){
                                calResult.put(columns, attr);
                                break;
                            }
                            else if(synonyms_str.length() == 1 && title.contains(synonyms_str)){
                                calResult.put(columns, attr);
                                break;
                            }
                        }
                    }
                }
            }
            else if(type.equals("enum")){
                JsonArray values = jsonObject.get("values").getAsJsonArray();
                Iterator valuesInterator = values.iterator();
                if (multiSelect){
                    String attrOriValuesString = record.getString(columns);
                    String[] attrOriValuesStrings;
                    if (null == attrOriValuesString){
                        attrOriValuesStrings = new String[0];
                    }
                    else {
                        attrOriValuesStrings = attrOriValuesString.split(",");
                    }
                    List<String> attrOriStringList = new ArrayList<>();
                    attrOriStringList = Arrays.asList(attrOriValuesStrings);
                    ArrayList<Float> arrtOriValues = new ArrayList<>();
                    for (String evalue : attrOriStringList){
                        arrtOriValues.add(Float.parseFloat(evalue));
                    }
                    ArrayList<String> matchResult = new ArrayList<>();
                    // for (Entry<String, JsonElement> attrEnum: values.entrySet()){
                    while (valuesInterator.hasNext()){
                        JsonObject attrEnum = (JsonObject) valuesInterator.next();
                        Float evalue = attrEnum.get("evalue").getAsFloat();
                        if (arrtOriValues.contains(evalue)) {
                            matchResult.add(evalue.toString());
                            continue;
                        }
                        Iterator attrEnumItor = attrEnum.get("synonyms").getAsJsonArray().iterator();
                        while (attrEnumItor.hasNext()) {
                            JsonElement enumSy = (JsonElement) attrEnumItor.next();
                            String enumSy_str = enumSy.toString();
                            if (enumSy_str.length() > 1 && content.contains(enumSy_str)) {
                                matchResult.add(evalue.toString());
                                break;
                            }
                            else if (enumSy_str.length() == 1 && title.contains(enumSy_str)) {
                                matchResult.add(evalue.toString());
                                break;
                            }
                        }

                    }
                    if (!matchResult.isEmpty()) {
                        calResult.put(columns, String.join(",", matchResult));
                    }
                }
                else {
                    Double attrEvalueDouble = record.getDouble(columns);
                    if (null != attrEvalueDouble && !attrEvalueDouble.isNaN()){
                        continue;
                    }
                    String matchEvalue = "";
                    //for (Entry<String, JsonElement> attrEnum: values.entrySet()){
                    while (valuesInterator.hasNext()){
                        JsonObject attrEnum = (JsonObject) valuesInterator.next();

                        if (!matchEvalue.isEmpty()) {
                            break;
                        }
                        Iterator attrEnumItor = attrEnum.get("synonyms").getAsJsonArray().iterator();
                        while (attrEnumItor.hasNext()) {
                            JsonElement enumSy = (JsonElement) attrEnumItor.next();
                            String enumSy_str = enumSy.toString();
                            if (enumSy_str.length() > 1 && content.contains(enumSy_str)) {
                                Float matchEvalueFloat = values.getAsJsonObject().get("evalue").getAsFloat();
                                matchEvalue = matchEvalueFloat.toString();
                                calResult.put(columns, matchEvalue);
                                break;
                            }
                            else if(enumSy_str.length() == 1 && title.contains(enumSy_str)){
                                Float matchEvalueFloat = values.getAsJsonObject().get("evalue").getAsFloat();
                                matchEvalue = matchEvalueFloat.toString();
                                calResult.put(columns, matchEvalue);
                                break;
                            }
                        }
                    }
                }
            }
        }
        this.tableOutput.setBigint("id", id);
        this.tableOutput.setString("result", gson.toJson(calResult, HashMap.class));
        System.out.println(calResult);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

    public Float getEvalueFather(Float evalue){
        Long nb = 1L;
        evalue = evalue * 1000F;
        for (Integer i=0; i<2; i++){
            if (evalue % nb != 0){
                return evalue - (evalue % nb);
            }
        }
        return evalue;
    }
}

/**
 * 字符串长度对比器
 */
class StringLengthComparator implements Comparator<String> {
    @Override
    public int compare(String o1, String o2) {
        if (o1.length() > o2.length()) {
            return -1;
        } else if (o1.length() == o2.length()) {
            return 0;
        } else {
            return 1;
        }
    }
}

/**
 *  evalue值排序
 */
class EvalueComparator implements Comparator<HashMap> {
    @Override
    public int compare(HashMap o1, HashMap o2) {
        if (!o1.containsKey("evalue") || !o2.containsKey("evalue")){
            return 0;
        }
        Float o1Evalue = (Float) o1.get("evalue");
        Float o2Evalue = (Float) o2.get("evalue");
        if (o1Evalue > o2Evalue){
            return 1;
        }
        else if (o1Evalue == o2Evalue) {
            return 0;
        }
        else {
            return -1;
        }
    }
}
