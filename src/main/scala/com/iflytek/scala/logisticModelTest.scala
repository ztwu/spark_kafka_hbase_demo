import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 1/12/18.
  */
object LogisticRegressionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LinearRegression").master("local").getOrCreate()
    val sc = spark.sparkContext

    //通过MLUtils工具类读取LIBSVM格式数据集
    val data = MLUtils.loadLibSVMFile(sc,"data/sample_libsvm_data.txt")

    //打印总数目
    print(s"data Count:${data.count}")
    val result =  data.randomSplit(Array(0.1,0.9),2L)
    val training = result(0)
    //打印训练数据数目
    print(s"training Count:${training.count}")
    val test = result(1)
    //打印测试数据数目
    print(s"test Count:${test.count}")

    //发现测试集和训练集并不一定按1：9的比例分

    //建立LogisticRegressionWithLBFGS对象，设置分类数 3 ，run传入训练集开始训练，返回训练后的模型
    val model = new LogisticRegressionWithLBFGS().setNumClasses(3).run(data)

    //使用训练后的模型对测试集进行测试，同时打印标签和测试结果
    val vectorsAndLabels= test.map{
      case LabeledPoint(l, f)=>{
        (l,model.predict(f))
      }
    }
    vectorsAndLabels.foreach(println)

  }
}