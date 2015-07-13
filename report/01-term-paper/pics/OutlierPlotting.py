import numpy as np 
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


normal_1x = np.random.rand(30) - 1
normal_1y = np.random.rand(30) + 2
normal_1z = 2 * np.random.rand(30) ** 2 + 1

normal_2x = np.random.rand(30) + 3
normal_2y = np.random.rand(30) + 1
normal_2z = 2 * np.random.rand(30) ** 2 + 12
outliers = np.array([[5, -4], [4, 7], [8, 10]])

def display_outliers3d():
	fig = plt.figure()
	ax = fig.add_subplot(111, projection='3d')



	N1 = np.array([normal_1x, normal_1y, normal_1z])
	N2 = np.array([normal_2x, normal_2y,normal_2z])
	markersize = 26
	
	norm1 = ax.scatter(N1[0,:], N1[1,:], N1[2,:], c='green', s=26)#, scatterpoints= 1)
	norm2 = ax.scatter(N2[0,:], N2[1,:], N2[2,:], s=26)#, scatterpoints=1)
	out = ax.scatter(outliers[0,:], outliers[1,:], outliers[2,:], c='red', marker='o', s=26)#, scatterpoints=1)
	ax.set_xticks([])
	ax.set_yticks([])
	ax.set_zticks([])
	plt.legend((norm1, norm2, out), ('Population 1', 'Population 2' ,'Outliers'))
	return ax, norm1, norm2, out
	# return plt

normal_1x_rec = normal_1x + 0.1 
normal_1y_rec = normal_1y - 0.2
normal_1z_rec = normal_1z + 0.25

normal_2x_rec = normal_2x + 0.1
normal_2y_rec = normal_2y - 0.2
normal_2z_rec = normal_2z + 0.1
outliers_rec = outliers +  np.array([[-1, 1], [0.6, 0.4], [0.8, 0.2]])#[:, np.newaxis]

def plot_reconstruction(ax, norm1, norm2, out):
	rec1 = ax.scatter(normal_1x_rec, normal_1y_rec, normal_1z_rec, marker='o', facecolors='none', edgecolors='green', label='Some label')
	rec2 = ax.scatter(normal_2x_rec, normal_2y_rec, normal_2z_rec, marker='o', facecolors='none', edgecolors='blue')
	out_rec = ax.scatter(outliers_rec[0,:], outliers_rec[1, :], marker='o', facecolors='none', edgecolors='r')
	ax.legend((norm1, norm2, out, rec1, rec2, out_rec), ('Population 1', 'Population 2', 'Outlier','Reconstruction 1', 'Reconstruction 2', 'Reconstruction outlier'), scatterpoints=1)

	# plt.legend((lo, ll, l, a, h, hh, ho),
 #           ('Low Outlier', 'LoLo', 'Lo', 'Average', 'Hi', 'HiHi', 'High Outlier'),
 #           scatterpoints=1,
 #           loc='lower left',
 #           ncol=3,
 #           fontsize=8)

def plot2D():
	N1 = np.array([np.random.rand(30) - 1, 2 * np.random.rand(30) ** 2 + 1])
	N2 = np.array([np.random.rand(30) + 2, 2 * np.random.rand(30) ** 2 + 12])
	outliers = np.array([[5, -4],[8, 10]])
	fig = plt.figure()
	ax = fig.add_subplot(111)
	sz = 26
	pop1 = ax.scatter(N1[0,:], N1[1,:], c='green', s=sz, label='Pop1 reduced')
	pop2 = ax.scatter(N2[0,:], N2[1,:], s=sz, label='Pop2 reduced')
	out = ax.scatter(outliers[0,:], outliers[1,:], c='red', marker='o', edgecolors='r', s=sz, label='Outliers reduced')
	# ax.legend(handles=(pop1, pop2, outliers), labels=('Pop1 reduced', 'Pop2 reduced', 'outliers reduced'), scatterpoints=1)
	frame = plt.gca()
	frame.axes.get_xaxis().set_ticks([])
	frame.axes.get_yaxis().set_ticks([])
	plt.legend(loc='upper left')
	# plt.show()
	plt.savefig(fig)

# plot2D()

ax, norm1, norm2, out = display_outliers3d()
plot_reconstruction(ax, norm1, norm2, out)
# ax.legend((norm1, norm2, out), ('Population 1', 'Population 2', 'Outlier'), scatterpoints=1)


plt.show()