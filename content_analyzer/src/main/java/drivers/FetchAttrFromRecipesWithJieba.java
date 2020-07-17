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
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.google.gson.*;
import com.huaban.analysis.jieba.*;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class FetchAttrFromRecipesWithJieba {

    public static void main(String[] args) throws OdpsException {
// args中，第一个参数是文章类型，第二个参数是pt分区

        if(args[0] == null){
            return;
        }

        String pt = args[0];

        JobConf job = new JobConf();

        // TODO: specify map output types
        // job.setMapOutputKeySchema(SchemaUtils.fromString( ?));
        // job.setMapOutputValueSchema(SchemaUtils.fromString( ?));

        //对于MapOnly的作业，必须显式设置reducer的个数为0。
        job.setNumReduceTasks(0);
        job.setNumMapTasks(2);
        LinkedHashMap<String, String> ptMap = new LinkedHashMap<String, String>();
        ptMap.put("pt", pt);

        // TODO: specify input and output tables
        InputUtils.addTable(TableInfo.builder().tableName("dim_article_recipes").build(), job);
        OutputUtils.addTable(TableInfo.builder().tableName("dim_output_fetch_recipes_attr").partSpec(ptMap).build(), job);

        // TODO: specify a mapper
        job.setMapperClass(attrWithJiebaMapper.class);
        // TODO: specify a reducer
        // job.setReducerClass( ?);

        RunningJob rj = JobClient.runJob(job);
        rj.waitForCompletion();
    }
}

class attrWithJiebaMapper extends MapperBase {
    public Record tableOutput;
    public JsonArray attr;
    public JiebaSegmenter segmenter = new JiebaSegmenter();

    @Override
    public void setup(TaskContext context) throws IOException {
        this.tableOutput = context.createOutputRecord();
        StringBuilder attrJsonBuild = new StringBuilder();
        BufferedInputStream bufferedInput = null;
        try{
            byte[] buffer = new byte[1024];
            int bytesRead = 0;
            bufferedInput = context.readResourceFileAsStream("attr.json");
            while ((bytesRead = bufferedInput.read(buffer)) != -1) {
                String chunk = new String(buffer, 0, bytesRead);
                attrJsonBuild.append(chunk);
            }
        }
        catch (FileNotFoundException ex){
            throw new IOException(ex);
        }
        catch (IOException ex){
            throw new IOException(ex);
        }


        String attrJson = attrJsonBuild.toString();
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
            if (!oneStep.get("content").isJsonNull()) {
                content = content.concat(oneStep.get("content").getAsString());
            }
        }
        List<SegToken> titleWordSegoken = this.segmenter.process(title, JiebaSegmenter.SegMode.INDEX);
        List<SegToken> contentWordSegoken = this.segmenter.process(content, JiebaSegmenter.SegMode.INDEX);
        List<String> titleWords = new ArrayList<>();
        List<String> contentWords = new ArrayList<>();
        for(SegToken segtoken : titleWordSegoken){
            titleWords.add(segtoken.word);
        }
        for(SegToken segtoken : contentWordSegoken){
            contentWords.add(segtoken.word);
        }
        Iterator attrInterator = this.attr.iterator();
        // for(Entry<String,; JsonElement> oneAttr : this.attr.entrySet()){
        while (attrInterator.hasNext()){
            JsonObject jsonObject = (JsonObject) attrInterator.next();
            String columns = jsonObject.get("column").getAsString();
            String type = jsonObject.get("type").getAsString();
            Boolean multiSelect = jsonObject.get("multi_select").getAsBoolean();
            if (type.equals("select")){
                JsonObject values = jsonObject.get("values").getAsJsonObject();
                // 当前字段是否多选
                if (multiSelect) {
                    ArrayList<String> attrCalValue = new ArrayList<>();
                    for (Map.Entry<String, JsonElement> value : values.entrySet()) {
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
                            if (synonyms.length() > 1 && contentWords.contains(synonyms)) {
                                attrCalValue.add(oneAttrValue);
                                break;
                            }
                            else if(synonyms.length() == 1 && titleWords.contains(synonyms)){
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
                    for(Map.Entry<String, JsonElement> value : values.entrySet()){
                        String attr = value.getKey();
                        JsonArray valueArray = value.getValue().getAsJsonArray();
                        Iterator valueIterator = valueArray.iterator();
                        while (valueIterator.hasNext()){
                            JsonElement synonyms = (JsonElement)valueIterator.next();
                            String synonyms_str = synonyms.getAsString();
                            if (synonyms_str.length() > 1 && contentWords.contains(synonyms_str)){
                                calResult.put(columns, attr);
                                break;
                            }
                            else if(synonyms_str.length() == 1 && titleWords.contains(synonyms_str)){
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
                        arrtOriValues.add(Float.valueOf(evalue));
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
                            String enumSy_str = enumSy.getAsString();
                            if (enumSy_str.length() > 1 && contentWords.contains(enumSy_str)) {
                                matchResult.add(evalue.toString());
                                break;
                            }
                            else if (enumSy_str.length() == 1 && titleWords.contains(enumSy_str)) {
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
                    String attrEvalue = record.getString(columns);

                    if (null != attrEvalue && !attrEvalue.isEmpty()){
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
                            String enumSy_str = enumSy.getAsString();
                            if (enumSy_str.length() > 1 && contentWords.contains(enumSy_str)) {
                                Float matchEvalueFloat = attrEnum.getAsJsonObject().get("evalue").getAsFloat();
                                matchEvalue = matchEvalueFloat.toString();
                                calResult.put(columns, matchEvalue);
                                break;
                            }
                            else if(enumSy_str.length() == 1 && titleWords.contains(enumSy_str)){
                                Float matchEvalueFloat = attrEnum.getAsJsonObject().get("evalue").getAsFloat();
                                matchEvalue = matchEvalueFloat.toString();
                                calResult.put(columns, matchEvalue);
                                break;
                            }
                        }
                    }
                }
            }
        }
        if(!calResult.isEmpty()) {
            this.tableOutput.setBigint("id", id);
            this.tableOutput.setString("result", gson.toJson(calResult, HashMap.class));
            context.write(this.tableOutput);
        }
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