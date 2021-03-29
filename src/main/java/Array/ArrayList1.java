package Array;


import java.util.*;
import java.util.List;
import java.util.UUID;
public class ArrayList1 {
    public static void main(String[] args) {

     Set<Integer> a= Collections.synchronizedSet(new TreeSet<>());
      //  Set<Integer> a=new TreeSet<>();
         for (int i=0;i<3;i++){
          Thread ss=   new Thread(new Runnable() {
                 @Override
                 public void run() {
                     a.add(1);
                     System.out.println(Thread.currentThread().getName() + "\t");
                     System.out.println(a);
                 }
             });
             ss.start();
         }



    }

}
