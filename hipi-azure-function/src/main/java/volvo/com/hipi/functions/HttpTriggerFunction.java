package volvo.com.hipi.functions;

import com.microsoft.aad.msal4j.*;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.AccessControlChangeCounters;
import com.azure.storage.file.datalake.models.AccessControlChangeResult;
import com.azure.storage.file.datalake.models.AccessControlType;
import com.azure.storage.file.datalake.models.PathAccessControl;
import com.azure.storage.file.datalake.models.PathAccessControlEntry;
import com.azure.storage.file.datalake.models.PathPermissions;
import com.azure.storage.file.datalake.models.PathRemoveAccessControlEntry;
import com.azure.storage.file.datalake.models.RolePermissions;
import com.azure.storage.file.datalake.options.PathSetAccessControlRecursiveOptions;
import volvo.com.hipi.helper.*;

import javax.servlet.http.HttpServletResponse;

/**
 * Azure Functions with HTTP Trigger.
 */
public class HttpTriggerFunction {
    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     */

    public static String CLIENT_ID = "bc7bfd79-e552-43c3-83f7-1f6554c8eaa4";
    public static String CLIENT_SECRET = "bc7bfd79-e552-43c3-83f7-1f6554c8eaa4";
    public static String AUTHORITY = "https://login.microsoftonline.com/f25493ae-1c98-41d7-8a33-0be75f5fe603";
    private DownloadFromAzure downloadFromAzure;
    private Logger log;

    public HttpTriggerFunction() throws URISyntaxException {
        downloadFromAzure = new DownloadFromAzure();

    }

    @FunctionName("HttpExample")
    public HttpResponseMessage run(
            @HttpTrigger(
                    name = "req",
                    methods = {HttpMethod.GET, HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS)
                    HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) throws Exception {
        log = context.getLogger();
        downloadFromAzure.setContextLog(log);
        log.info("Java HTTP trigger processed a request.");
        // Parse query parameter
        final String reportType = request.getQueryParameters().get("reportType");
        final String reportNo = request.getQueryParameters().get("reportNo");
        final String reportId = request.getQueryParameters().get("reportId");
        File file = null;
        // try {
        file = downloadFile(reportType, reportNo, reportId);
        log.info("Zip formed without exception : " + file.getAbsolutePath());
        log.info("Zip length: " + file.length());
        /*} catch (Exception e) {
            log.info("Error " +e.getMessage());
            e.printStackTrace();
        }*/

        log.info(file.getAbsolutePath());

        String zipName = reportNo + "_" + reportId;

        byte[] bytes = readFileToBytes(file);
        log.info("byte length " + bytes.length);
        HttpResponseMessage response = request.createResponseBuilder(HttpStatus.OK).body((Object) bytes).header("Content-Disposition", "attachment; filename=" + zipName + ".zip").build();
        file.delete();
        return response;

    }

    private static byte[] readFileToBytes(File file) throws IOException {

        byte[] bytes = new byte[(int) file.length()];

        FileInputStream fis = null;
        try {

            fis = new FileInputStream(file);

            //read file into bytes[]
            fis.read(bytes);

        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        return bytes;
    }


    private void Auth() throws MalformedURLException {
        IClientCredential credential = ClientCredentialFactory.createFromSecret(CLIENT_SECRET);
        ConfidentialClientApplication app = ConfidentialClientApplication
                .builder(CLIENT_ID, credential)
                .authority(AUTHORITY)
                .build();


    }


    public File downloadFile(String reportType, String reportno, String reportid) throws Exception {

        log.info("Download for file started" + reportid);
        List<File> files = null;
        FileOutputStream fos;
        ZipOutputStream zos;
        File zipFile = new File(reportid + ".zip");
        try {
            files = downloadFromAzure.getAttachmentsFromAzureBlob(reportno, reportid, reportType);
            fos = new FileOutputStream(zipFile);
            zos = new ZipOutputStream(fos);
            // create a list to add files to be zipped

            // package files
            for (File file : files) {
                // new zip entry and copying inputstream with file to
                // zipOutputStream, after all closing streams
           /*     zipOutputStream.putNextEntry(new ZipEntry(file.getName().substring(file.getName().indexOf("pr_mig"))));
                FileInputStream fileInputStream = new FileInputStream(file);
                IOUtils.copy(fileInputStream, zipOutputStream);
                fileInputStream.close();
                zipOutputStream.closeEntry();*/

                addToZipFile(file, zos);
                log.info("File to zip : " + file.getName());

            }
            zos.close();
            fos.close();
            // zipOutputStream.close();
            log.info("downlaod finish");
        } catch (Exception e) {
            String text = "Error occured sorry !! may be report does not have attachments";
            e.printStackTrace();
            throw e;
        } finally {
            if (files != null) {
                for (File file : files) {
                    if (file.exists()) {
                        file.delete();
                    }

                }
            }
        }

        return zipFile;
    }


    public static void addToZipFile(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {

        String fileName = file.getName().substring(file.getName().indexOf("pr_mig"));
        System.out.println("Writing '" + fileName + "' to zip file");
        System.out.println(file.getTotalSpace());
        FileInputStream fis = new FileInputStream(file);
        ZipEntry zipEntry = new ZipEntry(fileName);
        zos.putNextEntry(zipEntry);

        byte[] bytes = new byte[1024];
        int length;
        while ((length = fis.read(bytes)) >= 0) {
            zos.write(bytes, 0, length);

        }

        fis.close();
    }

}
