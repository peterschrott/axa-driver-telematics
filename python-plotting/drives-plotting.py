import numpy as np
import matplotlib.pyplot as plt
import os


def plot_drives(path_to_data= '../../data/drivers',driverId=1, outliers=False, exclude_drives=False):
    '''
    :param path_to_data: Path to the dataset
    :param driverId: The driver whose drives we
    :param outliers: Int IDs of outliers, if given, mark them in the plot
    :return:
    '''
    fig = plt.figure()
    ax = fig.add_subplot(111)
    r = lambda : np.random.randint(0, 255)
    path_to_driver = path_to_data + '/' + str(driverId)
    drives = os.listdir(path_to_driver)
    num_outliers = len(outliers)
    outlier_printed = False
    for drive in drives:
        print 'plotting ', str(drive)

        curr_drive = np.genfromtxt(path_to_driver+'/'+str(drive), delimiter=',',skiprows=1, dtype='float')
        if exclude_drives and int(drive.split('.')[0]) in exclude_drives:
            continue
        else:
            #outliers given
            if outliers:
                driveID = int(drive.split('.')[0])
                if driveID in outliers:
                    if outlier_printed:
                        ax.plot(curr_drive[:, 0], curr_drive[:,1], c='r')
                    else:
                        curr_plot_handle = ax.plot(curr_drive[:,0], curr_drive[:,1], c='r', label='First '+str(num_outliers)+' Outliers')
                        outlier_printed = True
                else:
                    rand_col = '#%02X%02X%02X' % (0,r(),r())
                    curr_plot_handle = ax.plot(curr_drive[:,0], curr_drive[:,1], c=rand_col, linestyle='--')
            else:
                rand_col = '#%02X%02X%02X' % (0,r(),r())
                curr_plot_handle = ax.plot(curr_drive[:,0], curr_drive[:,1], c=rand_col, linestyle='--')

        print curr_drive.shape
    ax.set_xticks([])
    ax.set_yticks([])
    plt.legend(loc='upper left')
    title = 'Trips of Driver '+str(driverId)
    if exclude_drives:
        title += ' exluding drives '+str(exclude_drives)
    plt.title(title)
    # plt.grid(True)
    plt.show()

def plot_all_drives(path_to_data= '../../data/drivers',driverId=1):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    r = lambda : np.random.randint(0, 255)
    path_to_driver = path_to_data + '/' + str(driverId)
    drives = os.listdir(path_to_driver)
    drive_length = dict()
    for drive in drives:
        print 'plotting ', str(drive)

        curr_drive = np.genfromtxt(path_to_driver+'/'+str(drive), delimiter=',',skiprows=1, dtype='float')
        rand_col = '#%02X%02X%02X' % (r(),r(),r())
        curr_plot_handle = ax.plot(curr_drive[:,0], curr_drive[:,1], c=rand_col)

    plt.legend(loc='upper left')
    # plt.grid(True)
    ax.set_xticks([])
    ax.set_yticks([])
    plt.title('Driver '+str(driverId))
    plt.show()


# plot all drives of driver 1, labeling 1, 69 and 183 as outliers
# plot_drives(driverId=1634, outliers=[136])
# plot_drives(driverId=1634)

# plot_all_drives(driverId=1080) #junk drives
# plot_all_drives(driverId=235) # not as much
# plot_all_drives(driverId=1809)#not as much
# plot_all_drives(driverId=1635)#one particularly long drive
# plot_all_drives(driverId=180)# not as much
# plot_all_drives(driverId=3506)# yup
# plot_all_drives(driverId=2267)# not as much, driving pattern though
# plot_all_drives(driverId=2818)# no junk, but some very long drives
# plot_all_drives(driverId=2193)
# plot_all_drives(driverId=2782)
# plot_all_drives(driverId=2417)



#plot_drives(driverId=3506, outliers=[196, 87, 139, 73])#, 139, 73,3])
# 1080 100 0.1349708921
# 1080 187 0.0281055497
# 1080 2 0.0264606246
# 1080 97 0.0221480216
# 1080 79 0.0190661134
# 1080 112 0.018655621

# plot_drives(driverId=1080, outliers=[100, 112,79,97,2,187])#, exclude_drives=[100])
plot_drives(driverId=1634, outliers=[136])

# plot_drives(driverId=18,outliers=[126,104,96,156,88])