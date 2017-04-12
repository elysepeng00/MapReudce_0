/*
 * Student Info: Name=Yujie Peng, ID=12237
 * Subject: CS570_HW3_Full_2016
 * Author: yujie
 * Filename: StudentGroupingComparator.java
 * Date and Time: Nov 2, 2016 12:46:40 PM
 * Project Name: YujiePeng_12237_CS557_HW3
 */
package npu.yujiepeng_12237_cs557_hw3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author yujie
 */
public class StudentGroupingComparator extends WritableComparator {
     public StudentGroupingComparator() {
        super(StudentCourseKey.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StudentCourseKey first = (StudentCourseKey) a;
        StudentCourseKey second = (StudentCourseKey) b;        
        return first.name.compareTo(second.name);
    }
    
}
