package com.mps.data.feed.ip;

import com.mps.utils.MyLogger;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class EngineIPAccountOld {
    
    private String filePath = "";
    private ConcurrentHashMap<String, String> ipRangeMap = new ConcurrentHashMap<String, String>();
    private ArrayList<DataIPAccount> IpDataList = new ArrayList<>();
    
    //default constructor
    public EngineIPAccountOld(String ipFeedFilePath) throws Exception {        
        if(ipFeedFilePath == null){
            MyLogger.log("IpEngine : NULL filePath");
            throw new Exception("IpEngine : NULL filePath");
        }
        this.filePath = ipFeedFilePath;
        
        MyLogger.log("IPEngine : START : " + this.filePath);
    }

    //method to process ip feed file for ipv4
    public ArrayList<DataIPAccount> processIpFile(String[] fileLines, String filePath) throws Exception {
        File file = null;
        InputStream fileInputStream = null;
        DataInputStream dataInputStream = null;
        BufferedReader bufferedReader = null;
        long lineNo = 0;
        String ips = "";
        String data = "";
            
        double iTracker = 0.0;

        try {
            iTracker = 1.0;            

            iTracker = 2.0;
            //getting file oject
            file = new File(filePath);
            if (!file.exists()) {
                iTracker = 3.0;
                throw new Exception("File Not Found!! : " + filePath);
            }

            iTracker = 4.0;
            //get object for fileinputstream
            fileInputStream = new FileInputStream(file);
            iTracker = 5.0;
            // Get the object of DataInputStream
            iTracker = 6.0;
            dataInputStream = new DataInputStream(fileInputStream);
            //get object for bufferreader
            iTracker = 7.0;
            bufferedReader = new BufferedReader(new InputStreamReader(dataInputStream, "UTF-8"));

            iTracker = 8.0;
            //Read File Line By Line            
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lineNo++;
                data = "";
                ips = "";
                
                try {
                    iTracker = 10.0;
                    //skipping first and blank lines
                    if (lineNo == 1 || line.trim().equalsIgnoreCase("")) {
                        continue;
                    }

                    iTracker = 11.0;
                    //splitting file line data into array for gatting the values
                    String[] lineData = line.split("\\|\\|\\|");
                    if (lineData == null || lineData.length < 6) {
                        iTracker = 12.0;
                        continue;
                    }else{
                        iTracker = 14.0;
                        //storing values from aplit array to variables for further use
                        //data = lineData[0].trim() + "~#~" + lineData[1].trim();
                        data = lineData[0].trim();
                        ips = lineData[2].trim();
                    }

                    //check for exisiting ip range
                    String tmpValue = ipRangeMap.get(ips);
                    if (tmpValue == null){
                        ipRangeMap.put(ips, data);
                    }else if (tmpValue != null && tmpValue.trim().equalsIgnoreCase(data)) {
                        iTracker = 15.0;
                        //MyLogger.log("Skipped Line for duplicate record : " + lineNo + " : " + line);
                        continue;
                    }else{
                        //putting the entry in IP Range map unique entries of IP
                        ipRangeMap.put(ips, tmpValue + "|||" + data);
                    }
                    
                    //getting start/end ip from an IP Range for IPv4 & IPv6
                    updateIpRangeData(ips, ipRangeMap.get(ips));
                    
                    //MyLogger.log(lineNo + " : " + lineData[2].trim() + " : " + ipset.toString());
                    iTracker = 70.0;
                } catch (Exception e) {
                    MyLogger.exception(lineNo + " : iTracker=" + iTracker + " : " + e);
                }

            }//while loop for each line End

            //
            return IpDataList;
            
        } catch (Exception e) {
            throw new Exception("processIpFile : " + lineNo + " : iTracker=" + iTracker + " : " + e.toString());
        } finally {
            try {bufferedReader.close();} catch ( NullPointerException e2) {}
            try {dataInputStream.close();} catch (NullPointerException e2) {}
            try {fileInputStream.close();} catch (NullPointerException e2) {}
        }
    }

    //method to get BigInteger Value of IPv4 and IPv6
    private BigInteger ipToBigInteger(String address) throws Exception{
        InetAddress ipAddress = null;
        byte[] bytes = null;
        try {
            ipAddress = InetAddress.getByName(address);
            bytes = ipAddress.getAddress();
            return new BigInteger(1, bytes);
        } catch (Exception e) {
            System.out.println("ipToBigInteger : " + address + " : " + e.toString());
            throw new Exception("ipToBigInteger : " + address + " : " + e.toString());
        }
    }
    
    // method to get low and high IP from IP Range
    public void updateIpRangeData(String ipRangeSet, String data) throws Exception{        
        
        BigInteger ipBegin;
        BigInteger ipEnd;
        
        String startIP = "";
        String endIP = "";
        int[][] ip1 = new int[1][2];
        int[][] ip2 = new int[1][2];
        int[][] ip3 = new int[1][2];
        int[][] ip4 = new int[1][2];
        double iTracker = 0.0;
        try {
            
            if(ipRangeSet == null){
                throw new Exception("NULL ipRageSet");
            }
            
            
            
            iTracker = 13.0;
            //IPv4 section * * * * * * * * * * * * * * * * * * * * *
            if (ipRangeSet.contains(".") && !ipRangeSet.contains(":")) {
                
                iTracker = 19.0;
                //replacing all / with - to make the code considtent
                ipRangeSet = ipRangeSet.replaceAll("/", "-");
                
                iTracker = 20.0;
                String[] ipRange = ipRangeSet.trim().split("\\.");
                if(ipRange.length != 4){
                    throw new Exception("Invalid IP Range : " + ipRangeSet);
                }
                
                iTracker = 25.0;
                //processing for part1 of IP
                String part1 = ipRange[0];
                if (part1.contains("-")) {
                    iTracker = 27.0;
                    int startPoint = Integer.parseInt(part1.split("-")[0]);
                    int endPoint = Integer.parseInt(part1.split("-")[1]);
                    ip1[0][0] = startPoint;
                    ip1[0][1] = endPoint;
                    
                    /*
                    ip1 = new int[endPoint - startPoint + 1];
                    int counter = 0;
                    iTracker = 28.0;
                    while ((startPoint + counter) <= endPoint) {
                        iTracker = 30.0;
                        ip1[counter] = startPoint + counter;
                        counter++;
                    }
                    */
                } else {
                    iTracker = 36.0;
                    ip1[0][0] = Integer.parseInt(part1);
                    ip1[0][1] = Integer.parseInt(part1);
                    //ip1 = new int[]{Integer.parseInt(part1)};
                }
                //processing for part2 of IP
                String part2 = ipRange[1];
                if (part2.contains("-")) {
                    iTracker = 37.0;
                    int startPoint = Integer.parseInt(part2.split("-")[0]);
                    int endPoint = Integer.parseInt(part2.split("-")[1]);
                    ip2[0][0] = startPoint;
                    ip2[0][1] = endPoint;
                    
                    /*
                    ip2 = new int[endPoint - startPoint + 1];
                    int counter = 0;
                    iTracker = 38.0;
                    while ((startPoint + counter) <= endPoint) {
                        iTracker = 39.0;
                        ip2[counter] = startPoint + counter;
                        counter++;
                    }
                    */
                } else {
                    iTracker = 40.0;
                    ip2[0][0] = Integer.parseInt(part2);
                    ip2[0][1] = Integer.parseInt(part2);
                    //ip2 = new int[]{Integer.parseInt(part2)};
                }
                //processing for part3 of IP
                iTracker = 41.0;
                String part3 = ipRange[2];
                if (part3.contains("-")) {
                    iTracker = 42.0;
                    int startPoint = Integer.parseInt(part3.split("-")[0]);
                    int endPoint = Integer.parseInt(part3.split("-")[1]);
                    ip3[0][0] = startPoint;
                    ip3[0][1] = endPoint;
                    
                    /*
                    ip3 = new int[endPoint - startPoint + 1];
                    int counter = 0;
                    iTracker = 44.0;
                    while ((startPoint + counter) <= endPoint) {
                        iTracker = 45.0;
                        ip3[counter] = startPoint + counter;
                        counter++;
                    }
                    */
                } else {
                    iTracker = 48.0;
                    ip3[0][0] = Integer.parseInt(part3);
                    ip3[0][1] = Integer.parseInt(part3);
                    //ip3 = new int[]{Integer.parseInt(part3)};
                }
                //processing for part4 of IP
                iTracker = 50.0;
                String part4 = ipRange[3];
                if (part4.contains("-")) {
                    iTracker = 51.0;
                    int startPoint = Integer.parseInt(part4.split("-")[0]);
                    int endPoint = Integer.parseInt(part4.split("-")[1]);
                    ip4[0][0] = startPoint;
                    ip4[0][1] = endPoint;
                    
                    /*
                    ip4 = new int[endPoint - startPoint + 1];
                    int counter = 0;
                    iTracker = 52.0;
                    while ((startPoint + counter) <= endPoint) {
                        iTracker = 54.0;
                        ip4[counter] = startPoint + counter;
                        counter++;
                    }
                    */
                } else {
                    iTracker = 55.0;
                    ip4[0][0] = Integer.parseInt(part4);
                    ip4[0][1] = Integer.parseInt(part4);
                    //ip4 = new int[]{Integer.parseInt(part4)};
                }
                
                //manipulating all ipv4 address for memory store                
                String ip = "";
                for (int i = ip1[0][0]; i <= ip1[0][1]; i++) {
                    iTracker = 61.0;
                    for (int j = ip2[0][0]; j <= ip2[0][1]; j++) {
                        iTracker = 62.0;
                        for (int k = ip3[0][0]; k <= ip3[0][1]; k++) {
                            iTracker = 63.0;
                            //check for complete range
                            
                            iTracker = 64.0;
                            startIP = "";
                            endIP = "";
                            if((ip3[0][0] == 0 && ip3[0][1] == 255) && (ip4[0][0] == 0 && ip4[0][1] == 255)){
                                startIP = i + "." + j + "." + ip3[0][0] + "." + ip4[0][0];
                                endIP = i + "." + j + "." + ip3[0][1] + "." + ip4[0][1];
                                //setting the IPRange values
                                DataIPAccount ipDataV4 = new DataIPAccount();
                                ipDataV4.ipType = 4;
                                ipDataV4.ipBegin = ipToBigInteger(startIP);
                                ipDataV4.ipEnd = ipToBigInteger(endIP);
                                ipDataV4.startIP = startIP;
                                ipDataV4.endIP = endIP;
                                ipDataV4.range = ipRangeSet;
                                ipDataV4.data = data;
                                this.IpDataList.add(ipDataV4);
                                break;
                            }else{
                                startIP = i + "." + j + "." + k + "." + ip4[0][0];
                                endIP = i + "." + j + "." + k + "." + ip4[0][1];
                            }
                            
                            //setting the IPRange values
                            DataIPAccount ipDataV4 = new DataIPAccount();
                            ipDataV4.ipType = 4;
                            ipDataV4.ipBegin = ipToBigInteger(startIP);
                            ipDataV4.ipEnd = ipToBigInteger(endIP);
                            ipDataV4.startIP = startIP;
                            ipDataV4.endIP = endIP;
                            ipDataV4.range = ipRangeSet;
                            ipDataV4.data = data;
                            this.IpDataList.add(ipDataV4);
                        }
                    }
                }//

                
            //IPv6 section * * * * * * * * * * * * * * * * * * * * *
            }else if (ipRangeSet.contains(":")) {
                iTracker = 13.0;
                InetAddress inetAddress;
                int prefixLength;
                DataIPAccount ipDataV6 = new DataIPAccount();
                //IpV6 Range
                if(ipRangeSet.contains(":/") || ipRangeSet.contains("::") || (ipRangeSet.contains(":") && ipRangeSet.contains("/"))) {
                    int index = ipRangeSet.indexOf("/");
                    String addressPart = ipRangeSet.substring(0, index);
                    String networkPart = ipRangeSet.substring(index + 1);
                    inetAddress = InetAddress.getByName(addressPart);
                    prefixLength = Integer.parseInt(networkPart);
                    ByteBuffer maskBuffer;
                    if (inetAddress.getAddress().length == 4) {
                        maskBuffer = ByteBuffer.allocate(4).putInt(-1);
                    } else {
                        maskBuffer = ByteBuffer.allocate(16).putLong(-1L).putLong(-1L);
                    }
                    BigInteger mask = (new BigInteger(1, maskBuffer.array())).not().shiftRight(prefixLength);

                    ByteBuffer buffer = ByteBuffer.wrap(inetAddress.getAddress());
                    BigInteger ipVal = new BigInteger(1, buffer.array());
                    
                    //setting the IPRange values
                    ipBegin = ipVal.and(mask);
                    ipEnd = ipBegin.add(mask.not());
                    startIP = "";
                    endIP = "";
                    
                    
                //Single IP
                }else{
                    ipBegin = ipToBigInteger(ipRangeSet);
                    ipEnd = ipToBigInteger(ipRangeSet);
                    startIP = ipRangeSet;
                    endIP = ipRangeSet;
                }
                
                
                ///setting ip data in IpData model
                ipDataV6.ipBegin = ipBegin;
                ipDataV6.ipEnd = ipEnd;
                ipDataV6.startIP = startIP;
                ipDataV6.endIP = endIP;
                ipDataV6.data = data;
                ipDataV6.range = ipRangeSet;
                ipDataV6.ipType = 6;
                IpDataList.add(ipDataV6);
                                
            //* * * * * * * * * * * * * * * * * * * * * * * * * * * *
            }else{
                //logical fail
                throw new Exception("Invalid IP Type! Logic Fail in Else");
            }
            
            //
            
        } catch (Exception e) {
            System.out.println("updateIpRangeData : " + e.toString());
            throw new Exception("updateIpRangeData : " + e.toString() + " : " + ipRangeSet);
        }
    }//end updateIpRangeData
    
    // method to get low and high IP from IP Range
    public BigInteger[] updateIpRangeData(String ipRangeSet) throws Exception{
        
        BigInteger[] ipRange = new BigInteger[2];
        String[] ipParts;
        String ipStart = "";
        String ipEnd = "";
        int ipType = 0;
        int ipPartNo = 0;
        double iTracker = 0.0;
        try {
            
            iTracker = 13.0;
            //IPv4 section * * * * * * * * * * * * * * * * * * * * *
            if (ipRangeSet.contains(".") && !ipRangeSet.contains(":")) {
                ipType = 4;
            
                iTracker = 20.0;
                ipParts = ipRangeSet.trim().split("\\.");
                
                iTracker = 25.0;
                if(ipParts.length != 4){
                    throw new Exception("Invalid IP Range : " + ipRangeSet);
                }
                
                //
                for(ipPartNo =0 ; ipPartNo < ipParts.length; ipPartNo++){                
                    //processing for part of IP
                    String ipPart = ipParts[ipPartNo];
                    if (ipPart.contains("-")) {
                        iTracker = 27.0;
                        int startPoint = Integer.parseInt(ipPart.split("-")[0]);
                        int endPoint = Integer.parseInt(ipPart.split("-")[1]);
                        //setting begin and end ip part
                        ipStart = ipStart + "." + startPoint;
                        ipEnd = ipEnd + "." + endPoint;
                    }else if (ipPart.contains("/")) {
                        iTracker = 27.0;
                        int startPoint = Integer.parseInt(ipPart.split("/")[0]);
                        int endPoint = Integer.parseInt(ipPart.split("/")[1]);
                        //setting begin and end ip part
                        ipStart = ipStart + "." + startPoint;
                        ipEnd = ipEnd + "." + endPoint;
                    } else {
                        iTracker = 36.0;
                        ipStart = ipStart + "." + ipPart;
                        ipEnd = ipEnd + "." + ipPart;
                    }
                    
                    //check for  format of IP address, removing the first start '.' from IP address
                    if(ipPartNo == 0){
                        ipStart = ipStart.substring(1);
                        ipEnd = ipEnd.substring(1);
                    }
                }//end of ip part loop
            
                
                //setting the IPRange values
                ipRange[0] = ipToBigInteger(ipStart);
                ipRange[1] = ipToBigInteger(ipEnd);
                
            //IPv6 section * * * * * * * * * * * * * * * * * * * * *
            }else if (ipRangeSet.contains(":")) {
                iTracker = 13.0;
                ipType = 6;
                InetAddress inetAddress;
                int prefixLength;

                //IpV6 Range
                if(ipRangeSet.contains(":/") || ipRangeSet.contains("::") || (ipRangeSet.contains(":") && ipRangeSet.contains("/"))) {
                        
                    
                //Single IP
                }else{
                    ipStart = ipRangeSet;
                    ipEnd = ipRangeSet;
                }
                
                int index = ipRangeSet.indexOf("/");
                String addressPart = ipRangeSet.substring(0, index);
                String networkPart = ipRangeSet.substring(index + 1);
                inetAddress = InetAddress.getByName(addressPart);
                prefixLength = Integer.parseInt(networkPart);
                ByteBuffer maskBuffer;
                if (inetAddress.getAddress().length == 4) {
                    maskBuffer = ByteBuffer.allocate(4).putInt(-1);
                } else {
                    maskBuffer = ByteBuffer.allocate(16).putLong(-1L).putLong(-1L);
                }
                BigInteger mask = (new BigInteger(1, maskBuffer.array())).not().shiftRight(prefixLength);

                ByteBuffer buffer = ByteBuffer.wrap(inetAddress.getAddress());
                BigInteger ipVal = new BigInteger(1, buffer.array());

                //BigInteger startIp = ipVal.and(mask);
                //BigInteger endIp = startIp.add(mask.not());
                //setting the IPRange values
                ipRange[0] = ipVal.and(mask);
                ipRange[1] = ipRange[0].add(mask.not());
                
                /*
                bytes = ipAddress.getAddress();
                return new BigInteger(1, bytes);
            
            
                IPAddressString string = new IPAddressString("1:2:3:4::/64");
                IPAddress subnet = string.getAddress();
                IPAddress highest = subnet.getHighest();
                */
                                
            //* * * * * * * * * * * * * * * * * * * * * * * * * * * *
            }else{
                //logical fail
                throw new Exception("Invalid IP Type! Logic Fail in Else");
            }
            
            
            
            return ipRange;
        } catch (Exception e) {
            System.out.println("getLowHighIpRanges : " + e.toString());
            throw new Exception("getLowHighIpRanges : " + e.toString());
        }
    }//end updateIpRangeData
    
    //run
    void run(){
        ArrayList<DataIPAccount> IpAccountDataList = null;
        long uniqueIpCount = 0;
        double iTracker = 0.0;
        try {
            iTracker = 1.0;
            //reading ip feed file into memory sets
            MyLogger.log("run : Processing Start");
            IpAccountDataList = processIpFile(null, this.filePath);
            uniqueIpCount = IpAccountDataList.size();
            MyLogger.log("run : Processing End : Total : " + uniqueIpCount + " : IPs stored in Collection.");
            
            //
            for(int ll =0 ; ll <IpAccountDataList.size(); ll++){
                //ipLogger.log(((IpData)IpAccountDataList.get(ll)).toString());
            }
            
            //inserting the Ip Feeds data in MongoDB for reference/backup purpose
            String tableName = "ip_feed_";
            
            
            MyLogger.log("run : IPAccountMapper : Start");
            //IPAccountMapper iPAccountMapper = new IPAccountMapper(IpAccountDataList);
            MapperIPAccount.initialize(IpAccountDataList);
            MyLogger.log("run : IPAccountMapper : End");
            
            iTracker = 5.0;
            //testing for IP
            ArrayList<DataIPAccount> results = null;
            String ip = "";
            
            iTracker = 6.0;
            // * * * * * * * * * * * * * * * * * * * * * * * *
            ip = "140.101.31.197";
            MyLogger.log(ip);
            results = MapperIPAccount.getIPAccountData(ip);
            MyLogger.log(ip + " : " + results.toString());
            
            iTracker = 7.0;
            // * * * * * * * * * * * * * * * * * * * * * * * *
            ip = "2001:200:180:0:ffff:0:0:ffff";
            MyLogger.log(ip);
            results = MapperIPAccount.getIPAccountData(ip);
            MyLogger.log(ip + " : " + results.toString());
            
            iTracker = 8.0;
            // * * * * * * * * * * * * * * * * * * * * * * * *
            ip = "130.37.121.121";
            MyLogger.log(ip);
            results = MapperIPAccount.getIPAccountData(ip);
            MyLogger.log(ip + " : " + results.toString());
            
            iTracker = 9.0;
            // * * * * * * * * * * * * * * * * * * * * * * * *
            ip = "2607:fb10:7035:ffff:0:ffff:ffff:0";
            MyLogger.log(ip);
            results = MapperIPAccount.getIPAccountData(ip);
            MyLogger.log(ip + " : " + results.toString());
            
            iTracker = 9.0;
            // * * * * * * * * * * * * * * * * * * * * * * * *
            ip = "245.19.185.22";
            MyLogger.log(ip);
            results = MapperIPAccount.getIPAccountData(ip);
            MyLogger.log(ip + " : " + results.toString());
            
        } catch (Exception e) {
           MyLogger.log("run : " + e);
        }                
    }
    
    //PSVM
    public static void main(String[] args) throws Exception{
        String filepat = "C:\\ksv\\insight\\feeds\\201703\\ip.institution.201703_11518.csv.org";
        try {
            new EngineIPAccountOld(filepat).run();
        } catch (Exception ex) {
            throw ex;
        }
    }
    
}
