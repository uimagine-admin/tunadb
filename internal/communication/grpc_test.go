  // Determine role based on environment or config
//   var nodeType pb.NodeType
//   if os.Getenv("IS_CLIENT") == "true" {
//     nodeType = pb.NodeType_CLIENT
//   } else {
//     nodeType = pb.NodeType_NODE
//   }

//   resp, err := client.sendRead(ctx,address, &pb.ReadRequest{
//     Date:     "3/11/2024",
//     PageId:   "1",
//     Columns:  []string{"event", "componentId", "count"},
//     Name:     os.Getenv("NODE_NAME"),
//     NodeType: nodeType,
//   })