package volvo.com.hipi.helper;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.Context;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.sas.AccountSasPermission;
import com.azure.storage.common.sas.AccountSasResourceType;
import com.azure.storage.common.sas.AccountSasService;
import com.azure.storage.common.sas.AccountSasSignatureValues;
import com.azure.storage.file.datalake.*;
import com.azure.storage.file.datalake.models.PathItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;


public class DownloadFromAzure {
	
	Logger log = LoggerFactory.getLogger(DownloadFromAzure.class);
	private java.util.logging.Logger logger;

	public void setContextLog(java.util.logging.Logger logger){
		this.logger =logger;
	}
	
	public Set<String> getBlobIdsForReportIdNum(String reportNo,String reportId,String reportType) throws Exception{
		System.out.println("getting blobids files for reportId : "+reportId);
		try{
		TableClient tableClient = AzureTableServiceClient.getHipiReportBlobdataMapingTableClient();
		if(tableClient == null){
			throw new Exception("Table not found");
		}
		System.out.println(tableClient);
			System.out.println(reportNo + "    "+reportId);
		TableEntity te = tableClient.getEntity(reportNo, reportId);
		System.out.println(te);
		if (te != null) {
			String blobids = (String) te.getProperty("blobdataid");
			if (blobids != null && !"".equals(blobids)) {
				return new HashSet<>(Arrays.asList(blobids.split(",")));
			}
		}
		} catch(TableServiceException e){
			e.printStackTrace();
			logger.info("Error :" +e.getMessage());
		}
	    return new HashSet<>();
	}


	/*public List<File> getAttachmentsFromAzureBlob(String reportNo, String reportId, String reportType) throws Exception {
		logger.info("Downloading files for report : "+reportNo);
		if(reportId == null ||reportId== null || reportType== null){
			throw new Exception("reportnumber , reportid and reportType required ");
		}
		Set<String> reportIdAttachmentBlobIds = new HashSet<>();//getBlobIdsForReportIdNum(reportNo,reportId,reportType);
		Date d = new Date(System.currentTimeMillis());
		//connectToUrl("https://portal.azure.com");
		//connectToUrl("https://portal.azure.com/hipifilesadlgen2.dfs.core.windows.net");
		String timestamp = d.toString().replaceAll(":", "_");
		DataLakeServiceClient dataLakeServiceClient = AzureBlobStorageClient.GetDataLakeServiceClient();



		DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient("hipi");
		System.out.println("file system "+dataLakeFileSystemClient.getFileSystemUrl());
		List<File> files = new ArrayList<>();


		DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient("Attachments").getSubdirectoryClient(reportType+"Files").getSubdirectoryClient(reportNo);


		System.out.println(directoryClient.getDirectoryUrl());
		logger.info("directoryClient: "+directoryClient.getDirectoryUrl());
		DataLakeFileClient fileClient ;
		File file;


		PagedIterable<PathItem> pagedIterable = directoryClient.listPaths();//directoryClient.listPaths(false, false, 100, java.time.Duration.ofMillis(100000l));
		logger.info("pagedIterable 1: "+pagedIterable);
		java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
		logger.info("PathItem: "+iterator);
		String filename;
		PathItem item;
		try{
			while (iterator.hasNext()) {
				item = iterator.next();
				if (item!=null && !item.isDirectory()) {
					System.out.println(item.getName());
					logger.info("filename from azure "+item.getName());
					filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
					if (matchingBlobId(reportIdAttachmentBlobIds, filename)) {
						System.out.println(filename);
						logger.info("filename from azure "+item.getName());
						fileClient = directoryClient.getFileClient(filename);
						file = new File(timestamp + filename);

						*//*OutputStream targetStream = new FileOutputStream(file);
						fileClient.read(targetStream);
						targetStream.close();
						fileClient.flush(file.length());*//*
						fileClient.readToFile(timestamp + filename, true);
						//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
						files.add(file);
					}

				} else if(reportId != null && !"".equals(reportId)) {
					String  dirName= item.getName().substring(item.getName().lastIndexOf("/") + 1);
					System.out.println(dirName );
					logger.info(dirName);
					addReportFilesFromSubDir(reportId, dirName, directoryClient, timestamp,files);
				}
			}

		} catch(Exception e){
			logger.info("Exception-azure : "+ e.getMessage());
			throw e;
		}
		return files;
	}
*/

	public List<File> getAttachmentsFromAzureBlob(String reportNo, String reportId, String reportType) throws Exception {
		logger.info("Downloading files for report : "+reportNo);
		if(reportId == null ||reportId== null || reportType== null){
			throw new Exception("reportnumber , reportid and reportType required ");
		}
		Set<String> reportIdAttachmentBlobIds = new HashSet<>();//getBlobIdsForReportIdNum(reportNo,reportId,reportType);
		Date d = new Date(System.currentTimeMillis());
		//connectToUrl("https://portal.azure.com");
		//connectToUrl("https://portal.azure.com/hipifilesadlgen2.dfs.core.windows.net");
		String timestamp = d.toString().replaceAll(":", "_");
		DataLakeServiceClient dataLakeServiceClient = AzureBlobStorageClient.GetDataLakeServiceClient();



		DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient("protusfiles");
		System.out.println("file system "+dataLakeFileSystemClient.getFileSystemUrl());
		List<File> files = new ArrayList<>();


		DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient(reportType+"Files").getSubdirectoryClient(reportNo);


		System.out.println(directoryClient.getDirectoryUrl());
		logger.info("directoryClient: "+directoryClient.getDirectoryUrl());
		DataLakeFileClient fileClient ;
		File file;

		String file1= "pr_mig_1092286_DQ6376M.pdf";
		String file2="pr_mig_204468_2009-02-18-LR-7018-Investigation.pdf";

		fileClient = directoryClient.getFileClient(file1);
		file = new File(timestamp + file1);

		fileClient.readToFile(timestamp + file1, true);
		//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
		files.add(file);

		fileClient = directoryClient.getFileClient(file2);
		file = new File(timestamp + file2);

		fileClient.readToFile(timestamp + file2, true);
		//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
		files.add(file);

		return files;
	}

	private void addReportFilesFromSubDir(String reportId, String dirName, DataLakeDirectoryClient directoryClient, String timestamp, List<File> files) {
		 DataLakeDirectoryClient client=directoryClient.getSubdirectoryClient(dirName).getSubdirectoryClient(reportId);
		 if(client != null){
			 logger.info("loading "+ dirName +"  for reportid  "+ reportId);
			 PagedIterable<PathItem> pagedIterable = client.listPaths();
			 java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
			String filename;
			File file;
			PathItem item;
			DataLakeFileClient fileClient;
			//try{
			 while (iterator.hasNext()) {
				  item = iterator.next();
					if (item!=null && !item.isDirectory()) {
						logger.info(item.getName());
						filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
						logger.info(filename);
						fileClient = client.getFileClient(filename);
						file = new File(timestamp + filename);
						//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
						fileClient.readToFile(timestamp + filename, true);
						files.add(file);
					} 
			}

			/*} catch(Exception e){
				e.printStackTrace();
				logger.info("Exception : "+ e.getMessage());
			}*/
		 }
	}


	private boolean matchingBlobId(Set<String> blobIds, String filename) {
		// pr_mig_1382506_ARGUS SR 1-13583102941.pdf
		/*
		 String blobid =filename.substring(7,filename.indexOf('_',7));
		 logger.info("**************check blob id in report blob set**** "+ blobid);
		 logger.info(blobIds.toString());
		return blobIds.contains(blobid);*/

		//Temp for by pass  filtering of attachment based on report num.
		return true;
		 
	}

	private static void copyInputStreamToFile(InputStream inputStream, File file)
	            throws IOException {

	        // append = false
	        try (FileOutputStream outputStream = new FileOutputStream(file, false)) {
	            int read;
	            byte[] bytes = new byte[8192];
	            while ((read = inputStream.read(bytes)) != -1) {
	                outputStream.write(bytes, 0, read);
	            }
	        }

	    }



	public List<File>  getDownloadUsingAzureBlobStorageClient() {
		String connectStr = "DefaultEndpointsProtocol=https;AccountName=hipifilesadlgen2;AccountKey=kBvu9bVMY53Pi+4u8RAxTPOVmxo/G2/4rtsiZZBgh36+p+637q1IeFL/gH2V52wd2m9aEcF6Qcux2c9M1haNWA==;EndpointSuffix=core.windows.net";
		List<File> files = new ArrayList<>();
		BlobContainerAsyncClient client = new BlobContainerClientBuilder()
				.connectionString(connectStr).containerName("protusfiles")
				.buildAsyncClient();

		BlobContainerClient bclient = new BlobContainerClientBuilder()
				.connectionString(connectStr).containerName("protusfiles")
				.buildClient();

		//bclient.getBlobClient("PPIFiles")
		System.out.println("***************************bclient**************" +bclient);
		logger.info("***************************bclient**************"+bclient);
		bclient.listBlobsByHierarchy("PPIFiles").forEach(blob ->
				System.out.printf("Name: %s, Directory? %b%n", blob.getName(), blob.isPrefix()));

		PagedIterable<BlobItem> itemsList = bclient.listBlobsByHierarchy("PPIFiles");
		BlobClient bc ;


		ListBlobsOptions op =new ListBlobsOptions();
		op.setPrefix("PPIFiles/100002/");
		itemsList =bclient.listBlobs(op, Duration.ofMillis(100000l));
		java.util.Iterator<BlobItem> iterator = itemsList.iterator();
		logger.info("iterator " +iterator.hasNext());
		BlobItem item;
		String filename;
		File file;
		Date d = new Date(System.currentTimeMillis());
		String timestamp = d.toString().replaceAll(":", "_");
		int count=0;
		while (iterator.hasNext()) {
			System.out.println("&&&&&&&&&&&&&&&");
			logger.info("&&&&&&&&&&&&&&&");
			item = iterator.next();
			if (item != null) {
				System.out.println(item.getName());
				logger.info(item.getName());
				bc =bclient.getBlobClient(item.getName());
				filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
				logger.info(filename);

				file = new File( timestamp+count+filename);
				bc.downloadToFile(file.getPath(),true);
				//copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
				files.add(file);
				count++;
			}
		}
		return files;
	}

}