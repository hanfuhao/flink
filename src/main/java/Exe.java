public class Exe {
    public static void main(String[] args) {
        System.out.println(Result());
    }
    public static int get(){
        try {
          return 1;

        }finally {
           return 2;
        }
    }
    public static int Result() {
        int x = 9 ;
        int y = 3;
        int res = 0;
        try {
            res = x/y;
            return res;
        }catch(Exception e)
        { return
                Integer.MAX_VALUE;
        }finally {

            res ++;//方法内部的到res值 所以不会影响 return后面 res
          System.out.println("finally语句块被执行了:"+res);
            }
    }
}
