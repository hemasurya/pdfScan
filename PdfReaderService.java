package org.spga.service.docuphase;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spga.dto.docuphase.PdfFields;
import org.spga.fileparser.Record;
import org.spga.service.azure.AzureStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.azure.storage.blob.BlobClient;

import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;

@Service
public class PdfReaderService {

	private static final Logger LOG = LoggerFactory.getLogger(PdfReaderService.class);
	private static final String TESSDATA_PREFIX = "tessdata";
	private final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyyy");

	@Value("${azure.storage.container-name}")
	private String containerName;
	
	@Value("${azure.storage.container-tessdata}")
	private String containerTesseract;
	
	@Autowired
	private AzureStorageService azureStorageService;

	/**
	 * Scan pdf.
	 *
	 * @param blobClient the blob client
	 * @param zipFilename the zip filename
	 * @param excelRecords the excel records
	 * @return the list
	 */
	public List<Record> scanPdf(BlobClient blobClient, String zipFilename, List<Record> excelRecords) {

		// filter forms of ("01721", "01848", "02050")
		LOG.info("Number of pdf files to process == {}", excelRecords.size());
		List<Record> recordList = new ArrayList<>();

		for (Record record : excelRecords) {
			String filename = record.getFileName();
			String formType = record.getFormNumber();
			try {
				Record rec = new Record();
				rec = record;
				byte[] byteData = extractZipFile(blobClient, filename);
				String ocrResult = processOCR(byteData);

				PdfFields pdfFields = mapFields(ocrResult, formType);
				rec.setPdfFields(pdfFields);
				rec.setZipFileName(zipFilename);
				recordList.add(rec);

			} catch (IOException | TesseractException e) {
				LOG.error("Error occured while processing pdf file == ", e);
			}
		}

		return recordList;
	}

	/**
	 * Map fields.
	 *
	 * @param ocrResult the ocr result
	 * @param formType the form type
	 * @return the pdf fields
	 */
	private PdfFields mapFields(String ocrResult, String formType) {
		PdfFields fields = new PdfFields();
		switch (formType) {
		case "01721":
			fields.setCusip(extractField(ocrResult, "CUSIP:", "Security Description:"));
			fields.setSecurityDescription(extractField(ocrResult, "Security Description:", "Trade Date:"));
			fields.setTradeDate(extractField(ocrResult, "Trade Date:", "Settlement Date:"));
			fields.setSettlementDate(extractField(ocrResult, "Settlement Date:", "Order Type:"));
			fields.setQuantity(extractField(ocrResult, "Quantity: (shares)", "Price:"));
			fields.setPrice(extractField(ocrResult, "Price:", "Commission:"));
			fields.setRequestType("Correction");
			fields.setReasonForCorrection(
					findCheckedField(ocrResult, "Request Date", "Origin of Error:", "(?<=\\s)(?=O |OQ |w |wy |y )"));
			String filterText = extractField(ocrResult, "Origin of Error:", "Charge fee to:");
			String originOfError = findByRegex(filterText, "&Y\\s?\\w+|&\\s?\\w+", "^&Y\\s*|^&\\s*");
			if (originOfError.equals("other"))
				originOfError = extractField(filterText, "other", "Charge fee to:");
			fields.setOriginOfError(originOfError);
			fields.setOrderType(findCheckedField(ocrResult, "Order Type:", "Quantity:" ,"(?<=\\s)(?=O |Y |Cf |& )"));
			fields.setRequestDate(LocalDate.now().format(DATE_FORMATTER));
			break;

		case "01848":
			fields.setCusip(extractField(ocrResult, "CUSIP/Symbol:", "Security Description:"));
			fields.setSecurityDescription(extractField(ocrResult, "Security Description:", "BondDesk"));
			fields.setTradeDate(extractField(ocrResult, "Trade Date:", "Settle Date:"));
			fields.setSettlementDate(extractField(ocrResult, "Settle Date:", "Order Type:"));
			fields.setQuantity(extractField(ocrResult, "Quantity:", "Price:"));
			fields.setPrice(extractField(ocrResult, "Price:", "Commission:"));
			fields.setRequestType(extractField(ocrResult, "Request Type:", "Reason for Correction"));
			fields.setOriginOfError(extractField(ocrResult, "Origin of Error:", "Charge Loss/Fee To:"));
			fields.setReasonForCorrection(
					findCheckedField(ocrResult, "Request Date", "Origin of Error:", "(?<=\\s)(?=O |OQ |w |wy |y )"));
			fields.setOrderType(findCheckedField(ocrResult, "Order Type:", "Quantity:" ,"(?<=\\s)(?=O |Y |Cf |& )"));
			fields.setRequestDate(LocalDate.now().format(DATE_FORMATTER));

			break;

		case "02050":
			fields.setCusip(extractField(ocrResult, "CUSIP/Symbol:", "Original Price:"));
			fields.setTradeDate(extractField(ocrResult, "Trade Date:", "Trade Number:"));
			fields.setQuantity(extractField(ocrResult, "Amount:", "Order Type:"));
			fields.setPrice(extractField(ocrResult, "Original Price:", "Trade Date:"));
			fields.setReasonForCorrection("Failed to cancel order");
			fields.setRequestType(extractField(ocrResult, "Type of Request:", "Origin of Error By:"));
			fields.setOriginOfError(extractField(ocrResult, "Origin of Error By:", "Charge Loss To:"));
			fields.setOrderType(extractField(ocrResult, "Order Type:", "Original To CUSIP"));
			fields.setRequestDate(LocalDate.now().format(DATE_FORMATTER));

			break;

		}
		return fields;
	}

	/**
	 * Extract field.
	 *
	 * @param ocrResult the ocr result
	 * @param startTag the start tag
	 * @param endTag the end tag
	 * @return the string
	 */
	private String extractField(String ocrResult, String startTag, String endTag) {
		int startIndex = ocrResult.indexOf(startTag);
		if (startIndex == -1)
			return "Not Found";

		startIndex += startTag.length();
		int endIndex = ocrResult.indexOf(endTag, startIndex);
		if (endIndex == -1)
			endIndex = ocrResult.length();

		return ocrResult.substring(startIndex, endIndex).trim();
	}
	
	/**
	 * Extract zip file.
	 *
	 * @param blobClient the blob client
	 * @param fileName the file name
	 * @return the byte[]
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws TesseractException the tesseract exception
	 */
	private byte[] extractZipFile(BlobClient blobClient, String fileName) throws IOException, TesseractException {
		String zipFile = azureStorageService.getBlobName(containerName); // Replace with your CSV file path
		BlobClient blob = azureStorageService.getBlobClient(zipFile, containerName);

		try (InputStream is = blob.openInputStream(); ZipInputStream zis = new ZipInputStream(is)) {

			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				System.out.println("Entry: " + entry.getName());
				if (null != entry.getName() && !entry.getName().endsWith(".csv")
						&& (entry.getName().substring(entry.getName().lastIndexOf('/') + 1)).equals(fileName))

				// fileNames.contains(entry.getName().substring(entry.getName().lastIndexOf('/')
				// + 1)))
				{
					ByteArrayOutputStream baos = new ByteArrayOutputStream();

					byte[] buffer = new byte[1024];
					int bytesRead;
					while ((bytesRead = zis.read(buffer)) != -1) {
						baos.write(buffer, 0, bytesRead);
					}
					byte[] pdfData = baos.toByteArray();

					return pdfData;

				}
				zis.closeEntry();
			}
			return null;
		}

	}

	/**
	 * Process OCR.
	 *
	 * @param bytes the bytes
	 * @return the string
	 */
	private String processOCR(byte[] bytes){
		String result ="";
		PDDocument document;
		try {
			document = Loader.loadPDF(bytes);
		
		LOG.info("Number of pages in pdf == {}", document.getNumberOfPages());
		PDFRenderer pdfRenderer = new PDFRenderer(document);

		BufferedImage image = pdfRenderer.renderImageWithDPI(0, 300); // Render at 300 DPI

		String tesseractPath = azureStorageService.downloadBlobFromAzure(containerTesseract, TESSDATA_PREFIX);
		ITesseract tesseract = new Tesseract();
		tesseract.setDatapath(tesseractPath);

		result = tesseract.doOCR(image);
		return result;
		} catch (IOException | TesseractException e) {
			LOG.error("Failed to perform OCR on PDF==", e);
		}
		return result;
	}

	/**
	 * Retreive check box.
	 *
	 * @param ocrResult the ocr result
	 * @param startTag the start tag
	 * @param endTag the end tag
	 * @param regex the regex
	 * @return the list
	 */
	private List<String> retreiveCheckBox(String ocrResult, String startTag, String endTag, String regex) {

		// For form 1721
		List<String> fields = new ArrayList<>();
		// filter reason for correction portion
		String filteredText = extractField(ocrResult, startTag, endTag);
		Pattern pattern = Pattern.compile(regex);
		String[] lines = filteredText.split("\n");

		for (String line : lines) {
			// Skip the first line (date)
			if (line.trim().matches("\\d{1,2}/\\d{1,2}/\\d{4}")) {
				continue;
			}

			String[] segments = pattern.split(line);

			// Add each field to list
			for (String segment : segments) {
				String trimmedSegment = segment.trim();
				if (!trimmedSegment.isEmpty()) {
					fields.add(trimmedSegment);
				}
			}
		}

		return fields;

	}

	/**
	 * Find checked field.
	 *
	 * @param ocrResult the ocr result
	 * @param startTag the start tag
	 * @param endTag the end tag
	 * @param regex the regex
	 * @return the string
	 */
	private String findCheckedField(String ocrResult, String startTag, String endTag, String regex) {
		List<String> fields = retreiveCheckBox(ocrResult, startTag, endTag, regex);
		Map<String, String> findMap = new HashMap<>(); // key: field value: prefix
		for (String field : fields) {
			if (field.startsWith("wy"))
				findMap.put(field, "wy");
			else if (field.startsWith("w"))
				findMap.put(field, "w");
			else if (field.startsWith("y"))
				findMap.put(field, "y");
			else if (field.startsWith("Y"))
				findMap.put(field, "Y");
			else if (field.startsWith("Cf"))
				findMap.put(field, "Cf");
			else if (field.startsWith("&"))
				findMap.put(field, "&");
		}

		if (!findMap.isEmpty()) {
			Entry entry = findMap.entrySet().iterator().next();
			return trimPrefix(entry.getKey().toString(), entry.getValue().toString());
		}
		return "";
	}

	/**
	 * Trim prefix.
	 *
	 * @param field the field
	 * @param prefix the prefix
	 * @return the string
	 */
	private String trimPrefix(String field, String prefix) {
		if (field.startsWith(prefix)) {
			return field.substring(prefix.length()).trim();
		}
		return field;
	}

	/**
	 * Find by regex.
	 *
	 * @param text the text
	 * @param regex the regex
	 * @param trimPrefix the trim prefix
	 * @return the string
	 */
	private String findByRegex(String text, String regex, String trimPrefix) {

		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(text);
		String matchedField = "";
		while (matcher.find()) {
			matchedField = matcher.group();
			System.out.println("Matched field: " + matchedField);
		}

		return matchedField.replaceFirst(trimPrefix, "").trim();
	}

}
