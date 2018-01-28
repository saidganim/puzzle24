package ida.ipl;

import java.io.Serializable;

public class Pair <K,V> implements Serializable{
    private K _key;
    private V _value;

    public Pair(K k, V v){
        _key = k;
        _value = v;
    }

    public void setKey(K k){
        _key = k;
    }

    public void setValue(V v){
        _value = v;
    }

    public K getKey(){
        return _key;
    }

    public V getValue(){
        return _value;
    }
}
