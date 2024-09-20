package org.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class MapReduce {

    private static String  in = "/user/hive/warehouse/ods.db/MeituanGuesthouse.txt";

    private static String  out = "/user/hive/warehouse/dwd.db/dwd_minsu_info";

//    private static String  out = "/user/hive/warehouse/dwd.db/ms";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        System.setProperty("hadoop.home.dir", "D:\\_PERSONAL\\Meituan_Project\\Meituan_Cleaning\\dep");

        if(args.length == 2){
            in = args[0];
            out = args[1];
        }

        System.setProperty("HADOOP_USER_NAME","root");

        Configuration config = new Configuration();
        FileUtilities.checkFileExists(config,new Path(out));

        Job job = Job.getInstance(config);

        job.setJarByClass(MapReduce.class);

        job.setMapperClass(msMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }


    /**
     * 数据清洗
     */

    static class msMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = line.split("-");

            try {
                if (fields.length == 22) {
                    String productId = fields[0];
                    String cityName = fields[1];
                    String title = fields[2].trim(); // 去掉标题两边的空格
                    String districtName = fields[3];
                    String locationArea = fields[4];

                    String starRating = fields[5];
                    String starRatingDesc = fields[6];
                    String commentNumber = fields[7];
                    String distanceDesc = fields[8].trim(); // 去掉详细地址两边的空格
                    String coverImage = fields[9];

                    String favCount = fields[10]; // 在新版中收藏和数量被后置了
                    String bedCount = fields[11];
                    String productUserCount = fields[12];
                    String consumeDesc = fields[13];

                    String avgFinalPrice = "0";
                    try {
                        // 先将字符串转换为整数类型，然后除以100得到新的价格
                         avgFinalPrice = String.valueOf(Integer.parseInt(fields[14].trim()) / 100.0);
                    } catch (NumberFormatException e) {
                        // 处理转换异常，打印错误日志
                        System.out.println("字段 avgFinalPrice 转换异常: " + fields[14] + ", 使用默认值 0");
                    }


                    String layoutRoom = fields[15];
                    String guestNumberDesc= fields[16];
                    String ugcDesc= fields[17];
                    String available= fields[18];
                    String isSuperHost = fields[19];

                    String tags = fields[20].replace("/", ""); // 标签字段格式化
                    //导入数据时意外加了个f，新版抓取已经改正了，下次抓取不用replacef了
                    String layoutDesc = fields[21].replace("f", "");

                    if ("0".equals(layoutRoom)) {
                        return;
                    }

                    Integer.parseInt(productId);
                    Integer.parseInt(starRating);
                    Integer.parseInt(favCount);
                    Integer.parseInt(productUserCount);
                    Integer.parseInt(commentNumber);

//                    String price = Integer.parseInt(discountPrice) / 100 + ""; // 把价格字段转成新的格式

                    String output = String.join("-",
                            productId, cityName, title, districtName, locationArea,
                            starRating, starRatingDesc, commentNumber, distanceDesc, coverImage,
                            favCount, bedCount, productUserCount, consumeDesc, avgFinalPrice,
                            layoutRoom, guestNumberDesc, ugcDesc,available,isSuperHost,
                            tags,layoutDesc);

                    System.out.println("输出数据: " + output); // 添加日志信息
                    context.write(NullWritable.get(), new Text(output));

                } else {
                    System.out.println("字段数量不正确: " + fields.length + " 行内容: " + line);
                }
            } catch (Exception ex) {
                System.out.println("异常数据: " + line + ", 异常原因: " + ex.getMessage());
                ex.printStackTrace(); // 打印完整堆栈信息
            }
        }
    }

}

