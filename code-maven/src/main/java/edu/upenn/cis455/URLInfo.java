package edu.upenn.cis455;

public class URLInfo {
    private String hostName;
    private int portNo;
    private String filePath;
    private boolean secure;

    /**
     * Constructor called with raw URL as input - parses URL to obtain host name and file path
     */
    public URLInfo(String docURL){
        if(docURL == null || docURL.equals(""))
            return;
        docURL = docURL.trim();
        int startLength = 0;
        if (docURL.startsWith("http://")) {
            startLength = 7;
            secure = false;
        } else if (docURL.startsWith("https://")) {
            startLength = 8;
            secure = true;
        } else {
            return;
        }
        if (docURL.length() <= startLength) {
            return;
        }
        // Stripping off 'http://' or 'https://'
        docURL = docURL.substring(startLength);

		/*If starting with 'www.' , stripping that off too
		if(docURL.startsWith("www."))
			docURL = docURL.substring(4);*/
        int i = 0;
        while(i < docURL.length()){
            char c = docURL.charAt(i);
            if(c == '/')
                break;
            i++;
        }
        String address = docURL.substring(0,i);
        if(i == docURL.length())
            filePath = "/";
        else
            filePath = docURL.substring(i); //starts with '/'
        if(address.equals("/") || address.equals(""))
            return;
        if(address.indexOf(':') != -1){
            String[] comp = address.split(":",2);
            hostName = comp[0].trim();
            try{
                portNo = Integer.parseInt(comp[1].trim());
            }catch(NumberFormatException nfe){
                portNo = 80;
            }
        }else{
            hostName = address;
            portNo = 80;
        }
    }

    public URLInfo(String hostName, String filePath, boolean secure){
        this.hostName = hostName;
        this.filePath = filePath;
        this.portNo = 80;
        this.secure = secure;
    }

    public URLInfo(String hostName,int portNo,String filePath, boolean secure){
        this.hostName = hostName;
        this.portNo = portNo;
        this.filePath = filePath;
        this.secure = secure;
    }

    public String getHostName(){
        return hostName;
    }

    public void setHostName(String s){
        hostName = s;
    }

    public int getPortNo(){
        return portNo;
    }

    public void setPortNo(int p){
        portNo = p;
    }

    public String getFilePath(){
        return filePath;
    }

    public void setFilePath(String fp){
        filePath = fp;
    }

    public boolean isSecure() {
        return secure;
    }

    @Override
    public String toString(){
        return (secure ? "https" : "http") + "://" + hostName + (portNo != 80 ? ":" + portNo : "") + filePath;
    }

}
