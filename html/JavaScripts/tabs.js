var currentPanel;
                                                                                                                                                     
function showPanel(panelNum) {
    //hide visible panel, show selected panel,
    //set tab
    if (currentPanel != null) {
     hidePanel();
    }
    document.getElementById ('panel'+panelNum).style.visibility = 'visible';
    currentPanel = panelNum;
    setState(panelNum);
}
                                                                                                                                                     
function hidePanel() {
    //hide visible panel, unhilite tab
    document.getElementById('panel'+currentPanel).style.visibility = 'hidden';
    document.getElementById('tab'+currentPanel).className = "tabOff";
    //document.getElementById
    //  ('tab'+currentPanel).style.backgroundColor =
    //  '#ffffff';
    //document.getElementById
    //  ('tab'+currentPanel).style.color = 'navy';
}
                                                                                                                                                     
function setState(tabNum) {
    if (tabNum==currentPanel) {
    document.getElementById('tab'+tabNum).className = "tabOn";
        // document.getElementById
        //   ('tab'+tabNum).style.backgroundColor =
        //   '#ddddff';
        // document.getElementById
        //   ('tab'+tabNum).style.color = 'red';
    } else {
        document.getElementById('tab'+tabNum).className = "tabOff";
        // document.getElementById
        //   ('tab'+tabNum).style.backgroundColor =
        //   '#ffffff';
        // document.getElementById
        //   ('tab'+tabNum).style.color = 'navy';
    }
}
                                                                                                                                                     
function hover(tab) {
    //tab.style.backgroundColor = 'ddddff';
}
