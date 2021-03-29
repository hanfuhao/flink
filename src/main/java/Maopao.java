import java.util.Arrays;

public class Maopao {
    public static void main(String[] args) {

       Double[] a=new Double[]{10.0,5.6,7.9,8.8,1.0,99.0};

        for(int i=0;i<a.length;i++){
                for(int j=0;j<a.length-1-i;j++){
                    if(a[j]>a[j+1]){
                        Double tmp=0.0;
                        tmp=a[j];
                        a[j]=a[j+1];
                        a[j+1]=tmp;

                    }
                }

        }
        System.out.println(Arrays.toString(a));
    }
}
