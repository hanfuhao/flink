package day6;

public class Person1 {
    private String name;
    private Double score;

    public Person1(){

    }

    public Person1(String name, Double score){
        super();
        this.name=name;
        this.score=score;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    @Override
    public String toString() {

        return "Person1{" +
                "name='" + name + '\'' +
                ", score=" + score +
                '}';
    }

}
