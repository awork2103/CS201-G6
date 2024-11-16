package edu.smu.smusql;

public class TestResult {

    public double insertTime;
    public long insertMem;
    
    public double selectTime;
    public long selectMem;

    public double updateTime;
    public long updateMem;

    public double deleteTime;
    public long deleteMem;

    public void updateInsert(double newTime , long newMem){
        insertTime += newTime;
        insertMem += newMem;
    }
    
    public void updateSelect(double newTime , long newMem){
        selectTime += newTime;
        selectMem += newMem;
    }

    public void updateUpdate(double newTime , long newMem){
        updateTime += newTime;
        updateMem += newMem;
    }

    public void updateDelete(double newTime , long newMem){
        deleteTime += newTime;
        deleteMem += newMem;
    }

    public void results(long numberOfTests){

        System.out.println("Average INSERT time across tests : " + insertTime/numberOfTests + " s");
        System.out.println("Average INSERT memory use across tests: " + insertMem/numberOfTests + " MB");
        
        System.out.println("Average SELECT time across tests : " + selectTime/numberOfTests + " s");
        System.out.println("Average SELECT memory use across tests: " + selectMem/numberOfTests + " MB");

        System.out.println("Average UPDATE time across tests : " + updateTime/numberOfTests + " s");
        System.out.println("Average UPDATE memory use across tests: " + updateMem/numberOfTests + " MB");

        System.out.println("Average DELETE time across tests : " + deleteTime/numberOfTests + " s");
        System.out.println("Average DELETE memory use across tests: " + deleteMem/numberOfTests + " MB");

        // System.out.println(insertTime/numberOfTests + " s");
        // System.out.println(insertMem/numberOfTests + " MB");
        
        // System.out.println(selectTime/numberOfTests + " s");
        // System.out.println(selectMem/numberOfTests + " MB");

        // System.out.println(updateTime/numberOfTests + " s");
        // System.out.println(updateMem/numberOfTests + " MB");

        // System.out.println(deleteTime/numberOfTests + " s");
        // System.out.println(deleteMem/numberOfTests + " MB");
    }

}
