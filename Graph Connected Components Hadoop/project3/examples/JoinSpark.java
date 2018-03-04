package edu.uta.cse6331;

import java.io.*;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

class Employee implements Serializable {
    private static final long serialVersionUID = 53L;
    public String name;
    public int dno;
    public String address;

    Employee ( String n, int d, String a ) {
        name = n; dno = d; address = a;
    }
}

class Department implements Serializable {
    private static final long serialVersionUID = 54L;
    public String name;
    public int dno;

    Department ( String n, int d ) {
        name = n; dno = d;
    }
}

public class JoinSpark {

    public static void main ( String[] args ) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Join");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Employee> e = sc.textFile(args[0])
            .map(new Function<String,Employee>() {
                    public Employee call ( String line ) {
                        Scanner s = new Scanner(line.toString()).useDelimiter(",");
                        return new Employee(s.next(),s.nextInt(),s.next());
                    }
                });
        JavaRDD<Department> d = sc.textFile(args[1])
            .map(new Function<String,Department>() {
                    public Department call ( String line ) {
                        Scanner s = new Scanner(line.toString()).useDelimiter(",");
                        return new Department(s.next(),s.nextInt());
                    }
                });
        JavaRDD<String> res = e.mapToPair(new PairFunction<Employee,Integer,Employee>() {
                    public Tuple2<Integer,Employee> call ( Employee e ) {
                        return new Tuple2<Integer,Employee>(e.dno,e);
                    }
                }).join(d.mapToPair(new PairFunction<Department,Integer,Department>() {
                        public Tuple2<Integer,Department> call ( Department d )  {
                            return new Tuple2<Integer,Department>(d.dno,d);
                        }
                    })).values()
            .map(new Function<Tuple2<Employee,Department>,String>() {
                            public String call ( Tuple2<Employee,Department> x ) {
                                return x._1.name+"  "+x._2.name;
                            }
                });
        res.saveAsTextFile(args[2]);
        sc.stop();
    }
}
