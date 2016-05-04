package src.main.java;

import java.util.ArrayList;
import java.util.List;

public class ServiceInfo {
    public String serviceName = "";
    public String serviceDescription = "";
    public String requirements = "";
    public List<String> taxonomyCodes = null;

    public ServiceInfo() {
        this.taxonomyCodes = new ArrayList<String>();
    }
}
