Public Function ProcessWebServiceRequest(requestItem As System.Dynamic.ExpandoObject, Writer As System.Action(Of String), CustomLogContainer As System.Text.StringBuilder) As String
    'Setting default config
    Dim t_destination As New QWTable()
    Dim t_origin As New QWTable()
    Dim response As System.XML.Linq.XElement
    Dim ResponseXMLContent As String = ""
    Dim requestItemDict As System.Collections.Generic.IDictionary(Of String, Object) = DirectCast(requestItem, System.Collections.Generic.IDictionary(Of String, Object))
    If Not requestItemDict.ContainsKey("LogResponseXMLContent") Then requestItemDict("LogResponseXMLContent") = True
    If Not requestItemDict.ContainsKey("LogRequestXMLStringAsSentToWebService") Then requestItemDict("LogRequestXMLStringAsSentToWebService") = True
    If Not requestItemDict.ContainsKey("DisposeQWTables") Then requestItemDict("DisposeQWTables") = False ' Never change this line, some code might rely on this behaviour.  Disposing manually is not necessary.
    If Not requestItemDict.ContainsKey("SetActiveToFalseOnQWTables") Then requestItemDict("SetActiveToFalseOnQWTables") = True ' Never change this line, some code might rely on this behaviour.  Disposing manually is not necessary.
    If Not requestItemDict.ContainsKey("CreateTimestampedFileAndManageRetention") Then requestItemDict("CreateTimestampedFileAndManageRetention") = True 
    CustomLogContainer.AppendLine("Logger start ProcessWebServiceRequest ----------:")
    ' Create Log action from Writer
    Dim getLogger As Func(Of String, System.Text.StringBuilder, Action(Of String), Action(Of String)) = DB.QWSession.Global.getLogger
    Dim Log As Action(Of String) = getLogger.Invoke("MV:", CustomLogContainer, Writer)
    Log("`Note:If Xml Strings are not visible, substitute tags symbols with [[ or ]]")
    Try
        ' Cast to dictionary for property access
        
        ' Replace parameters in XML template
        Dim replaceParameters As System.Action(Of System.Collections.Generic.IDictionary(Of String, Object)) = Sub(item As System.Collections.Generic.IDictionary(Of String, Object))
            Dim parametersXml As New System.Text.StringBuilder()
            Dim parametersDict As System.Collections.Generic.IDictionary(Of String, Object) = DirectCast(item("Parameters"), System.Collections.Generic.IDictionary(Of String, Object))
            For Each kvp As System.Collections.Generic.KeyValuePair(Of String, Object) In parametersDict
                parametersXml.AppendLine(String.Format("<{0}>{1}</{0}>", kvp.Key, kvp.Value))
            Next
            item("XmlString") = item("XmlString").ToString().Replace(":Parameters", parametersXml.ToString().TrimEnd())
        End Sub
        
        ' Replace parameters in the request item
        replaceParameters(requestItemDict)
        
        ' Parse and send SOAP request
        Dim xml As System.XML.Linq.XDocument = System.XML.Linq.XDocument.Parse(requestItemDict("XmlString").ToString())
        Dim webRequest__1 As System.Net.HttpWebRequest = DirectCast(System.Net.WebRequest.Create(requestItemDict("Url").ToString()), System.Net.HttpWebRequest)
        webRequest__1.Method = "POST"
        webRequest__1.ContentType = "text/xml; charset=UTF-8"
        webRequest__1.ContentLength = xml.ToString().Length
        
        ' Set credentials if Username and Password are provided
        If requestItemDict.ContainsKey("Username") AndAlso requestItemDict.ContainsKey("Password") AndAlso _
           requestItemDict("Username") IsNot Nothing AndAlso requestItemDict("Password") IsNot Nothing AndAlso _
           Not String.IsNullOrEmpty(requestItemDict("Username").ToString()) AndAlso Not String.IsNullOrEmpty(requestItemDict("Password").ToString()) Then
            webRequest__1.Credentials = New System.Net.NetworkCredential(requestItemDict("Username").ToString(), requestItemDict("Password").ToString())
        End If
        
        Using requestWriter2 As New System.IO.StreamWriter(webRequest__1.GetRequestStream())
            requestWriter2.Write(xml.ToString())
        End Using
        If requestItemDict.ContainsKey("LogRequestXMLStringAsSentToWebService") AndAlso requestItemDict("LogRequestXMLStringAsSentToWebService") = True Then
            Log("`Request XmlString as sent to web service: " & xml.ToString())
        End If
        ' Get response
        Dim ok As Boolean = True
        Dim errorMessage As String = ""
        
        Using resp As System.Net.HttpWebResponse = DirectCast(webRequest__1.GetResponse(), System.Net.HttpWebResponse)
            If resp.StatusCode = 200 Then
                Using responseStream As System.IO.Stream = resp.GetResponseStream()
                    Using reader As New System.IO.StreamReader(responseStream)
                        ResponseXMLContent = reader.ReadToEnd()
                        response = System.XML.Linq.XElement.Parse(ResponseXMLContent)
                    End Using
                End Using
                If requestItemDict.ContainsKey("LogResponseXMLContent") AndAlso requestItemDict("LogResponseXMLContent") = True Then
                    Log("` Risposta ResponseXMLContent : " + ResponseXMLContent)
                End If
            Else
                errorMessage = ":  - webservice Errore durante la connessione al WebService." + vbCrLf + _
                    "Status: " + LTrim(Str(resp.StatusCode)) + " " + resp.StatusDescription
                Log("`" + errorMessage)
                ok = False
            End If
        End Using

        If Not ok Then
            Return errorMessage
        End If

        Dim t As New System.Data.DataTable
        Dim c As System.Data.DataColumn
        Dim row As System.Data.DataRow

        Dim itemTagName As String = requestItemDict("ItemTagName").ToString()
        Dim itemElements As System.Collections.Generic.IEnumerable(Of System.Xml.Linq.XElement) = response.Descendants(DirectCast(System.Xml.Linq.XName.Get(itemTagName), System.Xml.Linq.XName))

        If Not itemElements.Any() Then
            Log("`Warning: No elements found with ItemTagName '" & itemTagName & "'")
            ' Return empty table or throw exception depending on requirements
        Else
            Dim firstItem As System.Xml.Linq.XElement = itemElements.First()
            Dim childElements As System.Collections.Generic.List(Of System.Xml.Linq.XElement) = firstItem.Elements().ToList()

            ' Check if columns are explicitly defined in requestItem
            Dim useExplicitColumns As Boolean = requestItemDict.ContainsKey("columns") AndAlso requestItemDict("columns") IsNot Nothing
            Dim explicitColumns As System.Collections.Generic.List(Of String) = Nothing

            If useExplicitColumns Then
                ' Convert columns to list of strings
                Dim columnsObj As Object = requestItemDict("columns")
                If TypeOf columnsObj Is System.Collections.IEnumerable AndAlso Not TypeOf columnsObj Is String Then
                    explicitColumns = New System.Collections.Generic.List(Of String)()
                    For Each col As Object In DirectCast(columnsObj, System.Collections.IEnumerable)
                        explicitColumns.Add(col.ToString())
                    Next
                    Log("`Info: Using explicit columns from requestItem.columns: " & String.Join(", ", explicitColumns))
                Else
                    Log("`Warning: columns property exists but is not a valid collection, ignoring")
                    useExplicitColumns = False
                End If
            End If

            If childElements.Count = 0 Then
                Log("`Warning: First item has no child elements")
            Else
                ' Check if this is a repeated-element structure
                Dim isRepeatedElement As Boolean = False
                Dim uniqueChildNames As System.Collections.Generic.HashSet(Of String) = New System.Collections.Generic.HashSet(Of String)(childElements.Select(Function(el As System.Xml.Linq.XElement) el.Name.ToString()))

                ' It's repeated if there's only one unique child element name and more than one occurrence
                isRepeatedElement = (uniqueChildNames.Count = 1 AndAlso childElements.Count >= 1)

                If isRepeatedElement Then
                    Log("`Info: Repeated tag mode. Only one unique child element name and more than one occurrence.")
                    ' Handle repeated element structure (like Serial tags)
                    Dim columnName As String = childElements(0).Name.ToString()
                    
                    ' Add attributes from parent as columns first (to avoid name conflicts)
                    Dim attributeNames As New System.Collections.Generic.List(Of String)
                    For Each attr As System.Xml.Linq.XAttribute In firstItem.Attributes()
                        Dim attrName As String = attr.Name.ToString()
                        
                        ' Handle attribute name conflict with element name
                        If attrName = columnName Then
                            attrName = attrName & "_Attr"
                            Log("`Warning: Attribute name '" & attr.Name.ToString() & "' conflicts with element name, renamed to '" & attrName & "'")
                        End If
                        
                        attributeNames.Add(attrName)
                        c = New System.Data.DataColumn
                        c.ColumnName = attrName
                        c.MaxLength = 500
                        t.Columns.Add(c)
                    Next
                    
                    ' Add the repeated element column
                    c = New System.Data.DataColumn
                    c.ColumnName = columnName
                    c.MaxLength = 500
                    t.Columns.Add(c)
                    
                    ' Process each item container
                    For Each itemEl As System.Xml.Linq.XElement In itemElements
                        ' Each repeated child element becomes a row
                        For Each childEl As System.Xml.Linq.XElement In itemEl.Elements()
                            row = t.NewRow
                            
                            ' Set element value (handle empty/null)
                            If String.IsNullOrEmpty(childEl.Value) Then
                                row(columnName) = System.DBNull.Value
                            Else
                                row(columnName) = childEl.Value.ToString()
                            End If
                            
                            ' Copy parent attributes to each row
                            Dim attrIndex As Integer = 0
                            For Each attr As System.Xml.Linq.XAttribute In itemEl.Attributes()
                                If String.IsNullOrEmpty(attr.Value) Then
                                    row(attributeNames(attrIndex)) = System.DBNull.Value
                                Else
                                    row(attributeNames(attrIndex)) = attr.Value.ToString()
                                End If
                                attrIndex += 1
                            Next
                            
                            t.Rows.Add(row)
                        Next
                    Next
                Else
                    ' Handle standard structure (different child elements = columns)
                    ' Build columns from explicit list or from first item
                    If useExplicitColumns Then
                        ' Use explicitly defined columns
                        For Each colName As String In explicitColumns
                            If Not t.Columns.Contains(colName) Then
                                c = New System.Data.DataColumn
                                c.ColumnName = colName
                                c.MaxLength = 500
                                t.Columns.Add(c)
                            End If
                        Next
                    Else
                        ' Auto-detect columns from first item
                        For Each f As System.Xml.Linq.XElement In childElements
                            If Not t.Columns.Contains(f.Name.ToString()) Then
                                c = New System.Data.DataColumn
                                c.ColumnName = f.Name.ToString()
                                c.MaxLength = 500
                                t.Columns.Add(c)
                            End If
                        Next
                    End If

                    ' Process each item as a row
                    For Each el As System.Xml.Linq.XElement In itemElements
                        row = t.NewRow
                        For Each col As System.Data.DataColumn In t.Columns
                            Dim element As System.Xml.Linq.XElement = el.Element(col.ColumnName)
                            If element IsNot Nothing AndAlso Not String.IsNullOrEmpty(element.Value) Then
                                row(col.ColumnName) = element.Value.ToString()
                            Else
                                row(col.ColumnName) = System.DBNull.Value
                            End If
                        Next
                        t.Rows.Add(row)
                    Next
                End If
            End If
        End If

        t.AcceptChanges()
        ' Call custom handler for Raw Or Lightly Processed Data if provided 
        If requestItemDict.ContainsKey("HandleRawOrLightlyProcessedData") AndAlso requestItemDict("HandleRawOrLightlyProcessedData") IsNot Nothing Then
            Dim resultHandler As System.Action(Of System.Xml.Linq.XDocument, System.Xml.Linq.XElement, System.Data.DataTable, System.Action(Of String)) = DirectCast(requestItemDict("HandleRawOrLightlyProcessedData"), System.Action(Of System.Xml.Linq.XDocument, System.Xml.Linq.XElement, System.Data.DataTable, System.Action(Of String)))
            resultHandler(xml, response, t, Log)
        End If
        ' Transform DataTable to QWTable
        t_origin.RequestLive = False
        t_origin.Table = t
        t_origin.Rowset = New QWRowset(t_origin)
        Log("`count:" & t_origin.Rowset.count())
        
        ' Setup and execute import if data exists
        If t_origin.Rowset.count() > 0 Then
            t_origin.rowset.First()
            
            ' Setup destination table
            t_destination.Database = DB
            t_destination.sql = requestItemDict("DestinationSQL").ToString()
            t_destination.allowallrecords = False
            t_destination.active = True
            
            ' Create virtual index only if DestinationVirtualIndexFields is provided
            If requestItemDict.ContainsKey("DestinationVirtualIndexFields") AndAlso requestItemDict("DestinationVirtualIndexFields") IsNot Nothing AndAlso Not String.IsNullOrEmpty(requestItemDict("DestinationVirtualIndexFields").ToString()) Then
                t_destination.Indexes("virtualIndex") = requestItemDict("DestinationVirtualIndexFields").ToString()
                t_destination.RowSet.SetIndex("virtualIndex")
            End If
            
            ' Call the custom import delegate (only handles data transfer) - pass Log as third parameter
            If requestItemDict.ContainsKey("ImportFromOriginIntoDestinationQWTable") AndAlso requestItemDict("ImportFromOriginIntoDestinationQWTable") IsNot Nothing Then
                Dim importDelegate As System.Action(Of QWTable, QWTable, System.Action(Of String)) = DirectCast(requestItemDict("ImportFromOriginIntoDestinationQWTable"), System.Action(Of QWTable, QWTable, System.Action(Of String)))
                importDelegate(t_origin, t_destination, Log)
            End If
        End If
        
        ' For testing: throw exception
        Throw New Exception("Error thrown manually just to test")
        
    Catch ex as Exception
        Log("`Import library exception: " & ex.Message)
        Throw
    Finally
        ' handling active state and disposing of t_origin
        If t_origin IsNot Nothing Then
            if requestItemDict.ContainsKey("SetActiveToFalseOnQWTables") AndAlso requestItemDict("SetActiveToFalseOnQWTables") = True Then ' Never change this line, some code might rely on this behaviour. Disposing manually is not necessary.
                t_origin.Active = False
            end if
            if requestItemDict.ContainsKey("DisposeQWTables") AndAlso requestItemDict("DisposeQWTables") = True Then ' Never change this line, some code might rely on this behaviour. Disposing manually is not necessary.
                t_origin.Dispose()
            Log("`t_origin were disposed you cannot reference them anymore.")
            end if
        End If
        ' handling active state and disposing of t_destination
        If t_destination IsNot Nothing Then
            if requestItemDict.ContainsKey("SetActiveToFalseOnQWTables") AndAlso requestItemDict("SetActiveToFalseOnQWTables") = True Then ' Never change this line, some code might rely on this behaviour. Disposing manually is not necessary.
                t_destination.active = False
            end if
            if requestItemDict.ContainsKey("DisposeQWTables") AndAlso requestItemDict("DisposeQWTables") = True Then ' Never change this line, some code might rely on this behaviour. Disposing manually is not necessary.
                t_destination.Dispose()
            Log("`t_destination were disposed you cannot reference them anymore.")
            end if
        End If
        Dim PathCartellaDocumentaleAmbiente as string = DB.QWSession.Q95_NOME_DIR_DOCUM
        Dim PathCartellaLogs as string = PathCartellaDocumentaleAmbiente & "\ImportLogs"
        Dim DebugNotes as string = "Error like: REVISIONE does not exist in query happens most likely because the first item does not contain the tag REVISIONE, because the columns are taken from the first row and the software assumes all rows have tghe same columns -- alll items ahve the same tags."
        ' Call the function
        if requestItemDict.ContainsKey("CreateTimestampedFileAndManageRetention") AndAlso requestItemDict("CreateTimestampedFileAndManageRetention") = True Then 
            Dim createdFilePath As String = DB.QWSession.Global.CreateTimestampedFileAndManageRetention(
            baseFolderPath:=PathCartellaLogs, ' "C:\Logs\MyApp"
            identifier:="ProcessWebServiceRequest_" & requestItemDict("Identifier"),
            data:= DebugNotes & vbCrLf & "Request--------------------- " & vbCrLf & requestItemDict("XmlString").ToString() & vbCrLf  & vbCrLf  & vbCrLf & "Response--------------------- " & vbCrLf & ResponseXMLContent & vbCrLf & "Log:" & vbCrLf &  CustomLogContainer.ToString(),
            numberOfFilesToKeep:=5,
            Writer:=Writer,
            CustomLogContainer:=CustomLogContainer
            )
        end if
    End Try
End Function
