[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared #"FHIR silver_condition" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "fc5da95e-4417-4632-848b-1677f3a195be"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "932ba5ef-0392-47ae-8d11-93ec8ac4fd57"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "FHIR.silver_condition", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "."]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseName = "LH_Silver"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dbo", ItemKind = "Schema"]}[Data]
in
  #"Navigation 3";
