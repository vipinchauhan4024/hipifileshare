package volvo.com.hipi.helper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.TableEntity;
import com.azure.data.tables.models.TableServiceException;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathItem;


public class DownloadFromAzure {
	
	Logger logger = LoggerFactory.getLogger(DownloadFromAzure.class);
	
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
			logger.error(e.getMessage());
		}
	    return new HashSet<>();
	}

	
	public List<File> getAttachmentsFromAzureBlob(String reportNo, String reportId, String reportType) throws Exception {
		logger.info("Downloading files for report : "+reportNo);
		if(reportId == null ||reportId== null || reportType== null){
			throw new Exception("reportnumber , reportid and reportType required ");
		}
		Set<String> reportIdAttachmentBlobIds =new HashSet<>();// getBlobIdsForReportIdNum(reportNo,reportId,reportType);
		reportIdAttachmentBlobIds.add("309909");
		reportIdAttachmentBlobIds.add("311941");

		Date d = new Date(System.currentTimeMillis());
		
		String timestamp = d.toString().replaceAll(":", "_");
		DataLakeServiceClient dataLakeServiceClient = AzureBlobStorageClient.GetDataLakeServiceClient();
		System.out.println(dataLakeServiceClient + "  "+ dataLakeServiceClient);
		DataLakeFileSystemClient dataLakeFileSystemClient = dataLakeServiceClient.getFileSystemClient("protusfiles");
		System.out.println(dataLakeFileSystemClient + "  "+ dataLakeFileSystemClient);
		List<File> files = new ArrayList<>();
		DataLakeDirectoryClient directoryClient = dataLakeFileSystemClient.getDirectoryClient(reportType+"Files").getSubdirectoryClient(reportNo);
		System.out.println("directoryClient   "+ directoryClient.getDirectoryName());
		PagedIterable<PathItem> pagedIterable = directoryClient.listPaths();
		java.util.Iterator<PathItem> iterator = pagedIterable.iterator();
		System.out.println("iterator   "+ iterator);
		DataLakeFileClient fileClient;
		File file;
		String filename;
		PathItem item;
		try{
		while (iterator.hasNext()) {
			  item = iterator.next();
			    System.out.println(item.getName());
				if (item!=null && !item.isDirectory()) {
                    
					System.out.println(item.getName());
					filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
					if (matchingBlobId(reportIdAttachmentBlobIds, filename)) {
						System.out.println(filename);
						fileClient = directoryClient.getFileClient(filename);
						file = new File(timestamp + filename);
						copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
						files.add(file);
					}
					
				} else if(reportId != null && !"".equals(reportId)) {
					 String  dirName= item.getName().substring(item.getName().lastIndexOf("/") + 1);
				     System.out.println(dirName );
				     addReportFilesFromSubDir(reportId, dirName, directoryClient, timestamp,files);
				}
		}

		} catch(Exception e){
			System.out.println(e.getMessage());
			e.printStackTrace();

		}
		System.out.println(files);
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
			try{
			 while (iterator.hasNext()) {
				  item = iterator.next();
					if (item!=null && !item.isDirectory()) {
						logger.info(item.getName());
						filename = item.getName().substring(item.getName().lastIndexOf("/") + 1);
						logger.info(filename);
						fileClient = client.getFileClient(filename);
						file = new File(timestamp + filename);
						copyInputStreamToFile(fileClient.openInputStream().getInputStream(), file);
						files.add(file);
					} 
			}

			} catch(Exception e){
				e.printStackTrace();
			}
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

}
