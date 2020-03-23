package com.athena.util;

import com.amazonaws.regions.Regions;
import software.amazon.awssdk.regions.Region;

import java.text.SimpleDateFormat;

public class Util {
    public static Region getRegion(String region) {
        switch (region) {
            case "AP_SOUTH_1":
                return Region.AP_SOUTH_1;
            case "EU_NORTH_1":
                return Region.EU_NORTH_1;
            case "EU_WEST_3":
                return Region.EU_WEST_3;
            case "EU_WEST_2":
                return Region.EU_WEST_2;
            case "EU_WEST_1":
                return Region.EU_WEST_1;
            case "AP_NORTHEAST_2":
                return Region.AP_NORTHEAST_2;
            case "AP_NORTHEAST_1":
                return Region.AP_NORTHEAST_1;
            case "US_GOV_EAST_1":
                return Region.US_GOV_EAST_1;
            case "CA_CENTRAL_1":
                return Region.CA_CENTRAL_1;
            case "SA_EAST_1":
                return Region.SA_EAST_1;
            case "CN_NORTH_1":
                return Region.CN_NORTH_1;
            case "US_GOV_WEST_1":
                return Region.US_GOV_WEST_1;
            case "AP_SOUTHEAST_1":
                return Region.AP_SOUTHEAST_1;
            case "AP_SOUTHEAST_2":
                return Region.AP_SOUTHEAST_2;
            case "EU_CENTRAL_1":
                return Region.EU_CENTRAL_1;
            case "US_EAST_1":
                return Region.US_EAST_1;
            case "US_EAST_2":
                return Region.US_EAST_2;
            case "US_WEST_1":
                return Region.US_WEST_1;
            case "CN_NORTHWEST_1":
                return Region.CN_NORTHWEST_1;
            case "US_WEST_2":
                return Region.US_WEST_2;
            case "AWS_GLOBAL":
                return Region.AWS_GLOBAL;
            case "AWS_CN_GLOBAL":
                return Region.AWS_CN_GLOBAL;
            case "AWS_US_GOV_GLOBAL":
                return Region.AWS_US_GOV_GLOBAL;
            default:
                return Region.AWS_US_GOV_GLOBAL;
        }
    }

        public static Regions getRegions(String region) {
            switch (region) {
                case "AP_SOUTH_1":
                    return Regions.AP_SOUTH_1;
                case "EU_NORTH_1":
                    return Regions.EU_NORTH_1;
                case "EU_WEST_3":
                    return Regions.EU_WEST_3;
                case "EU_WEST_2":
                    return Regions.EU_WEST_2;
                case "EU_WEST_1":
                    return Regions.EU_WEST_1;
                case "AP_NORTHEAST_2":
                    return Regions.AP_NORTHEAST_2;
                case "AP_NORTHEAST_1":
                    return Regions.AP_NORTHEAST_1;
                case "US_GOV_EAST_1":
                    return Regions.US_GOV_EAST_1;
                case "CA_CENTRAL_1":
                    return Regions.CA_CENTRAL_1;
                case "SA_EAST_1":
                    return Regions.SA_EAST_1;
                case "CN_NORTH_1":
                    return Regions.CN_NORTH_1;
                case "AP_SOUTHEAST_1":
                    return Regions.AP_SOUTHEAST_1;
                case "AP_SOUTHEAST_2":
                    return Regions.AP_SOUTHEAST_2;
                case "EU_CENTRAL_1":
                    return Regions.EU_CENTRAL_1;
                case "US_EAST_1":
                    return Regions.US_EAST_1;
                case "US_EAST_2":
                    return Regions.US_EAST_2;
                case "US_WEST_1":
                    return Regions.US_WEST_1;
                case "CN_NORTHWEST_1":
                    return Regions.CN_NORTHWEST_1;
                case "US_WEST_2":
                    return Regions.US_WEST_2;
                default:
                    return Regions.US_EAST_1;
            }
        }

    private static SimpleDateFormat simpleDateFormat;
    public static SimpleDateFormat getSimpleDateFormat(){
        if(simpleDateFormat == null) simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat;
    }
}
