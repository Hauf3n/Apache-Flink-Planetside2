package org.myorg.project.planetside2;

// Planetside regarding stuff

public class Planetside {

    public static String worldIdToName(int id){
        switch (id){
            case 2:
                return "Indar";
            case 6:
                return "Amerish";

            case 8:
                return "Esamir";

            default:
                return "Hossin";
        }
    }

    public static String serverIdToName(int id){
        switch (id){
            case 1:
                return "Connery";
            case 10:
                return "Miller";

            case 13:
                return "Cobalt";

            case 17:
                return "Emerald";

            case 40:
                return "SolTech";

            default:
                return "Unknown Server";
        }
    }

}
