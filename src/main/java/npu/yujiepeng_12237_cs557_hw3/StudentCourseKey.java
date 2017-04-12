/*
 * Student Info: Name=Yujie Peng, ID=12237
 * Subject: CS570_HW3_Full_2016
 * Author: yujie
 * Filename: StudentCourseKey.java
 * Date and Time: Nov 2, 2016 12:44:39 PM
 * Project Name: YujiePeng_12237_CS557_HW3
 */
package npu.yujiepeng_12237_cs557_hw3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author yujie
 */
public class StudentCourseKey implements WritableComparable<StudentCourseKey>{
    public Text dept; 
    public Text name;
    public Text course ;
    public DoubleWritable  count;

    public StudentCourseKey() {
        this.dept = new Text();
        this.course = new Text();
        this.name = new Text();
        this.count = new DoubleWritable();
    }

    public StudentCourseKey(Text dept, Text course, Text name, DoubleWritable count) {
        this.dept = dept; 
        this.course = course;
        this.name = name;
        this.count = count;
    }

    @Override
    public int hashCode() {
        return (this.dept).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StudentCourseKey))
            return false;
        StudentCourseKey other = (StudentCourseKey) o;
        return this.dept.equals(other.dept) ;
    }

    @Override
    public int compareTo(StudentCourseKey second) {
        if (this.name.toString().equals(second.name.toString()))
            return this.course.compareTo(second.course);
        else
            return this.name.compareTo(second.name);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        this.dept.write(out);  
        this.course.write(out);
        this.name.write(out);
        this.count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dept.readFields(in); 
        this.course.readFields(in);
        this.name.readFields(in);
        this.count.readFields(in);
    }
    
     public String toString() {
	return dept.toString() + " " + course.toString()+ " " + name.toString()+ " " + count.get();
    }
}
