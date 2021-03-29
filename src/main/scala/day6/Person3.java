package day6;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor//有参构造
@NoArgsConstructor  //无参构造
public class Person3 {
    private String name;
    private Double score;
}
