package volvo.com.hipi.helper;

public class Constants {

    public static final String SCOPES = "User.Read";

    private Constants(){}
    public final static String CLIENT_ID ="bc7bfd79-e552-43c3-83f7-1f6554c8eaa4";
    public final static String CLIENT_SECRET ="bc7bfd79-e552-43c3-83f7-1f6554c8eaa4";
    public final static String AUTHORITY = "https://login.microsoftonline.com/f25493ae-1c98-41d7-8a33-0be75f5fe603";
    public static final String HOME_PAGE = "https://hipifile.azurewebsites.net";
    public static final String REDIRECT_ENDPOINT = "/api/HttpExample";
    public static final String REDIRECT_URI = String.format("%s%s", HOME_PAGE, REDIRECT_ENDPOINT);

    public static final String SECRET = "8bc5791f-e12f-4ea7-bcd9-0801d14e2efa";
    public static final String SIGN_OUT_ENDPOINT = "/";
    public static final String POST_SIGN_OUT_FRAGMENT = "?post_logout_redirect_uri=";
    public static final Long STATE_TTL = 600l;
    public static final String SESSION_PARAM = "msalAuth";
    public static final String PROTECTED_ENDPOINTS = "/token_details, /call_graph";
    public static final String ROLES_PROTECTED_ENDPOINTS ="";
    public static final String ROLE_NAMES_AND_IDS = "";//Config.getProperty("app.roles");
    public static final String GROUPS_PROTECTED_ENDPOINTS = "";//Config.getProperty("app.protect.groups");
    public static final String GROUP_NAMES_AND_IDS ="";// Config.getProperty("app.groups");


    /*aad.clientId={enter-your-client-id-here}
aad.secret={enter-your-client-secret-here}
aad.authority=https://login.microsoftonline.com/{enter-your-tenant-id-here}
aad.scopes=User.Read

aad.signOutEndpoint=/oauth2/v2.0/logout/
aad.postSignOutFragment=?post_logout_redirect_uri=

# app.homePage is by default set to dev server address and app context path on the server
# for apps deployed to azure, use https://your-sub-domain.azurewebsites.net
app.homePage=http://localhost:8080/msal4j-servlet-graph

# endpoint for AAD redirect. Configured this to be the same as the URL pattern for AADRedirectServlet.java
app.redirectEndpoint=/auth/redirect

# app's state value validity in seconds
app.stateTTL=600
app.sessionParam=msalAuth
app.protect.authenticated=/token_details, /call_graph*/
}
