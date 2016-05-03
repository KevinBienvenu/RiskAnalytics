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
    print filename
    with open(filename+".txt", 'w') as openfile:
        openfile.write("name:"+name+"\n")
        openfile.write("xlabel:"+xlabel+"\n")
        openfile.write("ylabel:"+ylabel+"\n")
        openfile.write("typeyaxis:"+typeyaxis+"\n")
        Utils.drawArray(openfile, x, "x")
        Utils.drawArray(openfile, y1, "y1")
        openfile.write("name1:"+name1+"\n")
        openfile.write("percent:"+str(percent)+"\n")
        if y2!=None:
            Utils.drawArray(openfile, y2, "y2")
            openfile.write("name2:"+name2+"\n")
        if y3!=None:
            Utils.drawArray(openfile, y3, "y3")
            openfile.write("name3:"+name3+"\n")

def drawHistogramFromFile(filename):
    """
    Function that open the file filename that should contain histogram data
    and draw it using the function drawHistogram
    -- IN
    filename: the name of the file whithout the extention (string)
    -- OUT
    return nothing
    """
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
    with open(filename+".txt", 'r') as openfile:
        lines = openfile.readlines()
        for line in lines:
            tab = line.split(":")
            if tab[0] == "x":
                x = [i if len(str(i))==0 or str(i)[0]!="'" else i[1:len(i)-1] for i in tab[1].split(",")]
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
    drawHistogram(x=x, y1=y1, y2=y2, y3=y3, 
                  name1=name1, name2=name2, name3=name3, 
                  xlabel=xlabel, ylabel=ylabel, percent=percent,
                  typeyaxis=typeyaxis, name=name, filename=filename)
 
''' unused and non-tested functions '''  
           
def createHistogram2D(y0,
                      y1,
                      color0=Constants.colorBluePlotly,
                      color1=Constants.colorOrangePlotly,
                      name1="",
                      name2="",
                      xlabel="",
                      ylabel="",
                      name="Graphe Sans Titre",
                      filename="untitledPlot"):
    '''
    function that creates a histogram 2d with y0 on the x-axis, and y1 on the y-axis
    *unused and non-tested function*
    '''
    py.plotly.sign_in('KevinBienvenu','r8vjr5qj9n')
    trace0 = go.Histogram2d(x=y0, y=y1,  # @UndefinedVariable
                            histnorm='percent',
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
    

    
    
    
    
    
    
    
    
