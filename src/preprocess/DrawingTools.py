# -*- coding: utf-8 -*-
'''
Created on 13 Apr 2016

@author: Kévin Bienvenu

Module using plotly to draw nice charts.

=== functions : 
createHistogram(x,y1,y2,y3,color1,color2,color3,
                name1,name2,name3,xlabel,ylabel,typeyaxis,name,filename) 
                : function that saves or draws an histogram using plotly
drawHistogram(x,y1,y2,y3,color1,color2,color3,
                name1,name2,name3,xlabel,ylabel,typeyaxis,name,filename)
                : function that draw the histogram using plotly
saveHistogram(x,y1,y2,y3,color1,color2,color3,
                name1,name2,name3,xlabel,ylabel,typeyaxis,name,filename)
                : function that save a txt file containing all informations about the histogram
drawHistogramFromFile(filename): function that transform a txt file into a histogram then saves it as a .png file

createHistogram2D() : unused and non-tested function
'''

import Constants
import Utils
import plotly.graph_objs as go
import plotly.plotly as py
import numpy as np
from mpmath import norm

''' drawing and saving functions '''

def createHistogram(x,
                  y1,
                  y2=None,
                  y3=None,
                  color1=Constants.colorBluePlotly,
                  color2=Constants.colorOrangePlotly,
                  color3=Constants.colorGreenPlotly,
                  name1="",
                  name2="",
                  name3="",
                  percent=False,
                  xlabel="",
                  ylabel="",
                  typeyaxis="linear",
                  name="Graphe Sans Titre",
                  filename="untitledPlot"):
    '''
    Function that either store the graph locally or draw it directly 
    according to the boolean in Constant
    -- IN
    x : x labels in an array (str[] or int[])
    y1 : first data to plot in an array (int[])
    y2 : second data to plot in an array *optionnal (int[]) default = None
    y3 : third data to plot in an array *optionnal (int[]) default = None
    color1 : color to plot the data 1, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(28,173,228)'
    color2 : color to plot the data 2, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(39,206,213)'
    color3 : color to plot the data 3, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(62,136,83)'
    name1 : name of the data 1 (string) default = ""
    name2 : name of the data 2 (string) default = ""
    name3 : name of the data 3 (string) default = ""
    xlabel : name of the x-axis (string) default = ""
    ylabel : name of the y-axis (string) default = ""
    typeyaxis : type of y-axis must be 'linear' or 'log' (string) default = "linear"
    name : name of the graph (string) default = "Graphe Sans Titre"
    filename : name of the file where to store the final image of the graph, name whithout the extension (string) default = "untitledPlot"
    -- OUT
    returns nothing
    '''
    if(Constants.bToStoreBeforeDrawing):
        saveHistogram(x=x, y1=y1, y2=y2, y3=y3, 
                      color1=color1, color2=color2, color3=color3, 
                      name1=name1, name2=name2, name3=name3, 
                      percent=percent, xlabel=xlabel, ylabel=ylabel, 
                      typeyaxis=typeyaxis, name=name, filename=filename)
    else:
        drawHistogram(x=x, y1=y1, y2=y2, y3=y3, 
                      color1=color1, color2=color2, color3=color3, 
                      name1=name1, name2=name2, name3=name3, 
                      percent=percent, xlabel=xlabel, ylabel=ylabel, 
                      typeyaxis=typeyaxis, name=name, filename=filename)

def drawHistogram(x,
                  y1,
                  y2=None,
                  y3=None,
                  color1=Constants.colorBluePlotly,
                  color2=Constants.colorOrangePlotly,
                  color3=Constants.colorGreenPlotly,
                  name1="",
                  name2="",
                  name3="",
                  xlabel="",
                  ylabel="",
                  percent=False,
                  typeyaxis="linear",
                  name="Graphe Sans Titre",
                  filename="untitledPlot"):
    '''
    function that creates a histogram with up to three data to plot
    and saves it locally in a png file
    -- IN
    x : x labels in an array (str[] or int[])
    y1 : first data to plot in an array (int[])
    y2 : second data to plot in an array *optionnal (int[]) default = None
    y3 : third data to plot in an array *optionnal (int[]) default = None
    color1 : color to plot the data 1, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(28,173,228)'
    color2 : color to plot the data 2, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(39,206,213)'
    color3 : color to plot the data 3, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(62,136,83)'
    name1 : name of the data 1 (string) default = ""
    name2 : name of the data 2 (string) default = ""
    name3 : name of the data 3 (string) default = ""
    percent : boolean that settles if it is needed to normalize the values (boolean) default = False
    xlabel : name of the x-axis (string) default = ""
    ylabel : name of the y-axis (string) default = ""
    typeyaxis : type of y-axis must be 'linear' or 'log' (string) default = "linear"
    name : name of the graph (string) default = "Graphe Sans Titre"
    filename : name of the file where to store the final image of the graph, name whithout the extension (string) default = "untitledPlot"
    -- OUT
    returns nothing
    '''
    if x is None or y1 is None:
        print "error : no data to draw"
        return
    py.plotly.sign_in('KevinBienvenu','r8vjr5qj9n')
    if percent:
        y1 = [int(y) for y in y1]
        total = np.sum(y1)
        y1 = [100.0*y/total for y in y1]
        if y2!=None:
            y2 = [int(y) for y in y2]
            total = np.sum(y2)
            y2 = [100.0*y/total for y in y2]
        if y3!=None:
            y3 = [int(y) for y in y3]
            total = np.sum(y3)
            y3 = [100.0*y/total for y in y3]
#     trace1 = go.Bar(x=range(len(y1)),y=y1,name=name1,marker=dict(color=color1,line=dict(color='rgb(8,48,107)',width=1.5)))  # @UndefinedVariable
    trace1 = go.Bar(x=range(len(y1)),y=y1,name=name1,marker=dict(color=color1,line=dict(color='rgb(8,48,107)',width=1.5)))  # @UndefinedVariable
    data = [trace1]
    if(y2!=None):
        trace2 = go.Bar(x=range(len(y2)),y=y2,name=name2,marker=dict(color=color2,line=dict(color='rgb(8,48,107)',width=1.5)))  # @UndefinedVariable
        data.append(trace2)
    if(y3!=None):
        trace3 = go.Bar(x=range(len(y3)),y=y3,name=name3,marker=dict(color=color3,line=dict(color='rgb(8,48,107)',width=1.5)))  # @UndefinedVariable
        data.append(trace3)
    if str(x[0])[0]=="e":
        x = ['%.0e' % 10.0**float(xi[2]) if xi[-1]=="0" else "" for xi in x]
    layout = go.Layout(title=name,  # @UndefinedVariable
                       xaxis=dict(title = xlabel,
                                  tickmode="array",
                                  tickvals = range(len(x)),
                                  ticktext = x,
                                  tickangle = -70,
                                  tickfont=dict(size=14,color='rgb(107, 107, 107)')),
                       yaxis=dict(title=ylabel,
                                  type=typeyaxis,
                                  titlefont=dict(size=16,color='rgb(107, 107, 107)'),
                                  tickfont=dict(size=14,color='rgb(107, 107, 107)')),
                       legend=dict(x=0.6,
                                   y=1.1,
                                   bgcolor='rgba(255, 255, 255, 0)',
                                   bordercolor='rgba(255, 255, 255, 0)'),
                       barmode='group',
                       bargap=0.15,
                       bargroupgap=0.1)
    fig = go.Figure(data=data, layout=layout)  # @UndefinedVariable
#     fig = go.Figure(data=data)  # @UndefinedVariable
    py.image.save_as(fig, filename+".png")
#     print "ok"
#     py.plot(fig, filename=filename)

def saveHistogram(x,
                  y1,
                  y2=None,
                  y3=None,
                  color1=Constants.colorBluePlotly,
                  color2=Constants.colorOrangePlotly,
                  color3=Constants.colorGreenPlotly,
                  name1="",
                  name2="",
                  name3="",
                  percent=False,
                  xlabel="",
                  ylabel="",
                  typeyaxis="linear",
                  name="Graphe Sans Titre",
                  filename="untitledPlot"):
    """
    Function that stores locally an histogram in a txt file
    to be drawn later.
    -- IN
    x : x labels in an array (str[] or int[])
    y1 : first data to plot in an array (int[])
    y2 : second data to plot in an array *optionnal (int[]) default = None
    y3 : third data to plot in an array *optionnal (int[]) default = None
    color1 : color to plot the data 1, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(28,173,228)'
    color2 : color to plot the data 2, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(39,206,213)'
    color3 : color to plot the data 3, the format must be 'rgb(%r,%g,%b)' with %r,%g and %b in [0..255] (string) default = 'rgb(62,136,83)'
    name1 : name of the data 1 (string) default = ""
    name2 : name of the data 2 (string) default = ""
    name3 : name of the data 3 (string) default = ""
    percent : boolean that settles if it is needed to normalize the values (boolean) default = False
    xlabel : name of the x-axis (string) default = ""
    ylabel : name of the y-axis (string) default = ""
    typeyaxis : type of y-axis must be 'linear' or 'log' (string) default = "linear"
    name : name of the graph (string) default = "Graphe Sans Titre"
    filename : name of the file where to store the final image of the graph, name whithout the extension (string) default = "untitledPlot"
    -- OUT
    returns nothing
    """
    if x is None or y1 is None:
        print "error : no data to draw"
        return
    with open(filename+".txt", 'w') as openfile:
        openfile.write("name:"+name+"\n")
        openfile.write("xlabel:"+xlabel+"\n")
        openfile.write("ylabel:"+ylabel+"\n")
        openfile.write("typeyaxis:"+typeyaxis+"\n")
        Utils.drawArray(openfile, x, "x")
        Utils.drawArray(openfile, y1, "y1")
        openfile.write("name1:"+name1+"\n")
        openfile.write("percent:"+str(percent)+"\n")
        if y2 is not None:
            Utils.drawArray(openfile, y2, "y2")
            openfile.write("name2:"+name2+"\n")
        if y3 is not None:
            Utils.drawArray(openfile, y3, "y3")
            openfile.write("name3:"+name3+"\n")

def saveHistogram2D(y0,
                  y1,
                  xlabel="",
                  ylabel="",
                  name="Graphe Sans Titre",
                  filename="untitledPlot"):
    """
    Function that stores locally an 2d histogram in a txt file
    to be drawn later.
    -- IN
    y0 : data to plot on the x-axis in an array (int[])
    y1: data to plot ont the y-axis in an array (int[]) (must be same size as y0)
    xlabel : name of the x-axis (string) default = ""
    ylabel : name of the y-axis (string) default = ""
    name : name of the graph (string) default = "Graphe Sans Titre"
    filename : name of the file where to store the final image of the graph, name whithout the extension (string) default = "untitledPlot"
    -- OUT
    returns nothing
    """
    print filename
    with open(filename+".hist2d", 'w') as openfile:
        openfile.write("name:"+name+"\n")
        openfile.write("xlabel:"+xlabel+"\n")
        openfile.write("ylabel:"+ylabel+"\n")
        Utils.drawArray(openfile, y0, "y0")
        Utils.drawArray(openfile, y1, "y1")
        openfile.write("name:"+name+"\n")

def drawHistogramFromFile(filename, typeHist="bars"):
    """
    Function that open the file filename that should contain histogram data
    and draw it using the function drawHistogram
    -- IN
    filename: the name of the file whithout the extention (string)
    typeHist: string in ("bars","2D") precising the type of hist to draw (string) default = "bars"
    -- OUT
    return nothing
    """
    x=None
    y0=None
    y1=None
    y2=None
    y3=None
    name1=""
    name2=""
    name3=""
    xlabel=""
    ylabel=""
    percent = False
    typeyaxis="linear"
    name="Graphe Sans Titre"
    if typeHist=="bars":
        fn = filename+".txt"
    elif typeHist=="2d":
        fn = filename+".hist2d"
            
    with open(fn, 'r') as openfile:
        lines = openfile.readlines()
        for line in lines:
            tab = line.split(":")
            if tab[0] == "x":
                x = [i if len(str(i))==0 or str(i)[0]!="'" else i[1:len(i)-1] for i in tab[1].split(",")]
            if tab[0] == "y0":
                y0 = [i if len(str(i))==0 or str(i)[0]!="'" else i[1:len(i)-1] for i in tab[1].split(",")]
            if tab[0] == "y1":
                y1 = [i if len(str(i))==0 or str(i)[0]!="'" else i[1:len(i)-1] for i in tab[1].split(",")]
            if tab[0] == "y2":
                y2 = [i if len(str(i))==0 or str(i)[0]!="'" else i[1:len(i)-1] for i in tab[1].split(",")]
            if tab[0] == "y3":
                y3 = [i if len(str(i))==0 or str(i)[0]!="'" else i[1:len(i)-1] for i in tab[1].split(",")]
            if tab[0] == "name":
                name = tab[1]
            if tab[0] == "xlabel":
                xlabel = tab[1]
            if tab[0] == "ylabel":
                ylabel = tab[1]
            if tab[0] == "name1":
                name1 = tab[1]
            if tab[0] == "name2":
                name2 = tab[1]
            if tab[0] == "name3":
                name3 = tab[1]
            if tab[0] == "typeyaxis":
                typeyaxis = tab[1]
            if tab[0] == "percent":
                percent = tab[1][:4]=="True"
    if typeHist=="bars":
        drawHistogram(x=x, y1=y1, y2=y2, y3=y3, 
                      name1=name1, name2=name2, name3=name3, 
                      xlabel=xlabel, ylabel=ylabel, percent=percent,
                      typeyaxis=typeyaxis, name=name, filename=filename)
    elif typeHist=="2d":
        createHistogram2D(y0=y0, y1=y1,
                          xlabel = xlabel, ylabel = ylabel,
                          name = name, filename = filename)
           
def createHistogram2D(y0,
                      y1,
                      xlabel="",
                      ylabel="",
                      xbins = None,
                      ybins = None,
                      zmin = 0,
                      zmax = 0,
                      histnorm = "percent",
                      name="Graphe Sans Titre",
                      filename="untitledPlot"):
    '''
    function that creates a histogram 2d with y0 on the x-axis, and y1 on the y-axis
    warning : y0 and y1 must be of the same length
    --IN:
    y0 : array of absisses of points (array[float/int])
    y1 : array of ordinates of points (array[float/int])
    xlabel : name of the x-axis (string) default = ""
    ylabel : name of the y-axis (string) default = ""
    name : name of the graph (string) default = "Graphe Sans Titre"
    filename : name of the file where to store the final image of the graph, name whithout the extension (string) default = "untitledPlot"
    -- OUT
    returns nothing
    '''
    # testing the length condition
    if len(y0)!=len(y1):
        print "error : y0 and y1 have different sizes"
        return
    py.plotly.sign_in('KevinBienvenu','r8vjr5qj9n')
    if zmin==zmax:
        trace0 = go.Histogram2d(x=y0, y=y1,  # @UndefinedVariable
                                autobinx=xbins is None,
                                xbins=xbins,
                                autobiny=xbins is None,
                                ybins=ybins,
                                histnorm=histnorm,
                                colorscale=[[0, 'rgb(12,51,131)'], [0.25, 'rgb(10,136,186)'], [0.5, 'rgb(242,211,56)'], [0.75, 'rgb(242,143,56)'], [1, 'rgb(217,30,30)']]
                                )
    else:
        trace0 = go.Histogram2d(x=y0, y=y1,  # @UndefinedVariable
                                autobinx=xbins is None,
                                xbins=xbins,
                                autobiny=xbins is None,
                                ybins=ybins,
                                histnorm=histnorm,
                                zmin=zmin,
                                zmax=zmax,
                                zauto=False,
                                colorscale=[[0, 'rgb(12,51,131)'], [0.25, 'rgb(10,136,186)'], [0.5, 'rgb(242,211,56)'], [0.75, 'rgb(242,143,56)'], [1, 'rgb(217,30,30)']]
                                )
        
    data = [trace0]
    layout = go.Layout(title=name,  # @UndefinedVariable
                       xaxis=dict(title = xlabel,
                                  tickfont=dict(size=14,color='rgb(107, 107, 107)')),
                       yaxis=dict(title=ylabel,
                                  titlefont=dict(size=16,color='rgb(107, 107, 107)'),
                                  tickfont=dict(size=14,color='rgb(107, 107, 107)')),
                       )
    fig = go.Figure(data=data,layout=layout)  # @UndefinedVariable
    py.image.save_as(fig, filename+".png")

def drawLargeHistogram2D(filename):
    '''
    experimental function - do not use for general purpose !
    '''
    nbLabel0 = 25
    nbLabel1 = 25
    with open(filename+".hist2d", "r") as fichier:
        for line in fichier:
            tab = line.split(":")
            if tab[0]=="y0":
                y0 = tab[1].split(",")
            elif tab[0]=="y1":
                y1 = tab[1].split(",")
            elif tab[0] == "name":
                name = tab[1]
            elif tab[0] == "xlabel":
                xlabel = tab[1]
            elif tab[0] == "ylabel":
                ylabel = tab[1]
            del tab
    min0 = int(min(y0))
    max0 = 800000
    min1 = int(min(y1))
    max1 = int(max(y1))
    label0 = [min0+i*(max0-min0)/nbLabel0 for i in range(nbLabel0)]
    label1 = [min1+i*(max1-min1)/nbLabel1 for i in range(nbLabel1)]
    hist = [[0]*nbLabel1 for _ in range(nbLabel0)]
    for k in range(len(y0)):
        i=0
        while i<nbLabel0-1 and int(y0[k])>int(label0[i]):
            i+=1
        j=0
        while j<nbLabel1-1 and int(y1[k])>int(label1[j]):
            j+=1
        hist[i][j] += 1
#         print i,j,y0[k]
    y0bis = []
    y1bis = []
    for i in range(nbLabel0):
        for j in range(nbLabel1):
            hist[i][j] = 1000.0*hist[i][j]/len(y0)
            for _ in range(int(hist[i][j])):
                y0bis.append(label0[i])
                y1bis.append(label1[j])
    createHistogram2D(y0=y0bis, y1=y1bis, xlabel=xlabel, ylabel=ylabel, name=name, filename=filename)
    
def createHistogram2DFromArray(array,
                               xbins = None,
                               ybins = None,
                               norm=False,
                               xlabel="",
                               ylabel="",
                               zmin=0,
                               zmax=0,
                               name="Graphe Sans Titre",
                               filename="untitledPlot"): 
    '''
    TODO: doc à rédiger plus tard
    ''' 
    print "drawing :",name
    if norm:
        norm = array.sum()
        array = 1000.0*array/norm+0.5
    array = array.astype(np.int16)
    y0 = []
    y1 = []
    if xbins is None:
        xscale = range(len(array)) 
    else:
        xscale = np.arange(xbins['start'],xbins['end']+xbins['size'],xbins['size'])
    if ybins is None:
        yscale = range(len(array[0])) 
    else:
        yscale = np.arange(ybins['start'],ybins['end']+ybins['size'],ybins['size'])
    
    shape = array.shape
    for i in range(1,shape[0]):
        for j in range(1,shape[1]):
            y0 += [xscale[i]] * array[i][j]
            y1 += [yscale[j]] * array[i][j]
    createHistogram2D(y0 = y0, y1 = y1, 
                      xlabel = xlabel, ylabel = ylabel, 
                      xbins = xbins, ybins = ybins,
                      zmin=zmin,zmax=zmax, 
                      histnorm="",
                      name = name,
                      filename = filename)
    
    
    
    
    
    
